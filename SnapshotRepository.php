<?php

declare(strict_types=1);

namespace Storm\AggregateRepository;

use Doctrine\DBAL\Exception;
use Storm\AggregateRepository\Exception\AggregateTypeMismatch;
use Storm\AggregateRepository\Snapshot\SnapshotStore;
use Storm\Chronicler\Directory\StreamHeadStore;
use Storm\Chronicler\Exception\InvalidPosition;
use Storm\Chronicler\Exception\NotADomainEvent;
use Storm\Chronicler\Exception\StorageFailure;
use Storm\Chronicler\Query\AggregateStreamSince;
use Storm\Chronicler\Store\StreamReader;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Aggregate\AggregateRepository;
use Storm\Contracts\Aggregate\AggregateRoot;
use Storm\Contracts\Aggregate\InvalidSnapshotState;
use Storm\Contracts\Aggregate\SnapshotableAggregateRoot;
use Storm\Contracts\Clock\ClockExceptionContract;
use Storm\Contracts\Serializer\SerializationExceptionContract;
use Storm\Stream\StreamName;
use Throwable;

/**
 * Snapshot-accelerated `AggregateRepository` decorator.
 *
 * Wraps the inner `DefaultAggregateRepository` for an aggregate that implements
 * `SnapshotableAggregateRoot`: `retrieve()` loads the latest snapshot and replays only the events
 * after it, via `AggregateStreamSince`; a missing or stale snapshot falls back to a full
 * reconstitution through the inner repository.
 *
 * `store()` is delegated unchanged; the write path stays pure. Snapshots are produced off the hot path
 * by the sweep, never in `store()`. The manager wires this only for snapshotable aggregates.
 *
 * @implements AggregateRepository<AggregateIdentity, AggregateRoot<AggregateIdentity>>
 *
 * @see DefaultAggregateRepository
 * @see \Storm\Contracts\Aggregate\SnapshotableAggregateRoot
 */
final readonly class SnapshotRepository implements AggregateRepository
{
    /**
     * @param  AggregateRepository<AggregateIdentity, AggregateRoot<AggregateIdentity>>  $inner
     * @param  class-string<SnapshotableAggregateRoot<AggregateIdentity>>  $aggregateClass
     * @param  class-string<AggregateIdentity>  $idClass
     */
    public function __construct(
        private AggregateRepository $inner,
        private SnapshotStore $snapshots,
        private StreamReader $streamReader,
        private StreamHeadStore $heads,
        private string $aggregateClass,
        private string $idClass,
        private string $category,
    ) {}

    public function store(AggregateRoot $aggregate): void
    {
        $this->inner->store($aggregate); // write path stays pure; the sweep produces snapshots
    }

    /**
     * {@inheritDoc}
     *
     * Snapshot-cache infrastructure failures also cross as `StorageFailure`, a decision rather than
     * an inherited clause: degrading a broken cache READ to a miss would turn every load into a full
     * replay that hides the outage, so only a corrupt cache ROW degrades, never a failing cache.
     */
    public function retrieve(AggregateIdentity $id): ?AggregateRoot
    {
        // the same runtime keeper the inner repository carries: the template TId only exists in
        // phpdoc, and the snapshot-hit path below never reaches the inner guard while every check
        // it does run is STRING-based, so a foreign identity class whose string collides would be
        // served a hydrated aggregate instead of this refusal
        if ($id::class !== $this->idClass) {
            throw AggregateTypeMismatch::id($this->idClass, $id::class);
        }

        $stream = new StreamName($this->category)->withQualifier($id->toString())->toString();

        // the snapshot load is a read too: the port reserves Throwable for a storage failure, so any
        // conforming adapter's failure crosses as the contracted StorageFailure, not raw, the same
        // boundary discipline as the tail read below.
        try {
            $snapshot = $this->snapshots->load($stream);
        } catch (Throwable $e) {
            throw StorageFailure::wrap(sprintf('the %s snapshot load', $this->category), $e);
        }

        // a snapshot whose type doesn't match this repository's aggregate, a category collision, is a
        // cache miss, not state to restore; fall through to a type-safe full reconstitution.
        if ($snapshot === null || $snapshot->aggregateType !== $this->aggregateClass) {
            return $this->inner->retrieve($id);
        }

        // the snapshot tail read crosses the port as the contracted StorageFailure, cause preserved,
        // matching the inner full-reconstitution path; the replay's own corruption and an unknown
        // stored event type propagate distinctly.
        try {
            $aggregate = ($this->aggregateClass)::fromSnapshot(
                $id,
                $snapshot->state,
                $snapshot->version,
                AggregateHistoryReplay::validated(
                    $this->aggregateClass,
                    $id,
                    $this->streamReader->retrieveByFilter(new AggregateStreamSince($this->category, $stream, $snapshot->version)),
                    $snapshot->version,
                ),
            );
        } catch (InvalidSnapshotState) {
            // the aggregate's own strict restoreState refused the state bag, a corrupt CACHE row,
            // never a read failure: discard and replay the authoritative events. Anything else out of
            // restoreState, a TypeError or logic bug, stays loud on purpose.
            return $this->inner->retrieve($id);
        } catch (Exception|InvalidPosition|SerializationExceptionContract|ClockExceptionContract|NotADomainEvent $e) {
            throw StorageFailure::wrap(sprintf('the %s snapshot tail read', $this->category), $e);
        }

        // The coherence guard: an EMPTY tail, version unchanged, is the one case where nothing proved
        // the authoritative stream still AGREES with the snapshot. One primary-key probe of the head,
        // and only on the empty-tail path, since a non-empty tail is itself the proof:
        // - Head 0, absent, = the ORPHAN: the stream was erased, the snapshot is pure cache, and the
        //   full replay says null, never a phantom;
        // - Head < snapshot version = the RECREATION: the same id was re-created and lives a new,
        //   shorter life; the old snapshot is a stale cache of a DEAD history, discard and replay the
        //   new one. The erase-time snapshot deletion is the root closure; this guard is the read-side
        //   backstop for the crash window and hand-managed stores.
        if ($aggregate !== null
            && $aggregate->version() === $snapshot->version
            && $this->heads->lastVersion($stream) < $snapshot->version) {
            return $this->inner->retrieve($id);
        }

        // a stale snapshot shape or a corrupt version is discarded, since fromSnapshot returned null,
        // so full reconstitution instead
        return $aggregate ?? $this->inner->retrieve($id);
    }
}
