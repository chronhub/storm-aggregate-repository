<?php

declare(strict_types=1);

namespace Storm\AggregateRepository;

use Doctrine\DBAL\Exception;
use Storm\AggregateRepository\Exception\AggregateTypeMismatch;
use Storm\AggregateRepository\Exception\CorruptStreamHistory;
use Storm\AggregateRepository\Exception\InspectionVersionOutOfRange;
use Storm\Chronicler\Exception\NotADomainEvent;
use Storm\Chronicler\Exception\StorageFailure;
use Storm\Chronicler\Query\AggregateStreamUpTo;
use Storm\Chronicler\Store\StreamReader;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Aggregate\CorruptAggregateHistory;
use Storm\Contracts\Aggregate\SnapshotableAggregateRoot;
use Storm\Contracts\Chronicler\InvalidPosition;
use Storm\Contracts\Chronicler\UnknownEventType;
use Storm\Contracts\Clock\ClockExceptionContract;
use Storm\Contracts\Serializer\SerializationExceptionContract;
use Storm\Stream\StreamCategory;
use Storm\Stream\StreamName;

/**
 * Read-only diagnostic fold of one snapshotable aggregate at a past version.
 *
 * The fold replays from version 1 through `AggregateStreamUpTo`, so the target bounds the SQL read
 * itself, and every record passes the same `AggregateHistoryReplay` guard as the repository. The
 * answer is a `HistoricalAggregateState`, never the aggregate: the inspector holds a `StreamReader`
 * and nothing that appends, stores or snapshots, and it reads no snapshot either, since a snapshot
 * only exists at the version it was taken and can never shorten a fold that must stop earlier.
 *
 * Two limits belong to the answer:
 *
 * - It is today's fold of past events: current aliases, upcasters and apply methods run, not the
 *   code that was deployed when those events were recorded
 *
 * - An erase and re-creation of the same identity between the caller's head observation and this
 *   read would fold the new life's events; the bound excludes later appends, not a replaced stream
 *
 * Not autowired: an instance is bound to one aggregate class, id class and category, built by
 * `AggregateRepositoryManager::inspectorFor()`.
 */
final readonly class HistoricalAggregateInspector
{
    /**
     * The deepest version a historical inspection folds, on the requested version rather than on
     * the stream head, since the target is what bounds the replay.
     */
    public const int MAX_VERSIONS = 100_000;

    /**
     * @param  class-string<SnapshotableAggregateRoot<AggregateIdentity>>  $aggregateClass
     * @param  class-string<AggregateIdentity>  $idClass
     */
    public function __construct(
        private string $aggregateClass,
        private string $idClass,
        private StreamCategory $category,
        private StreamReader $streamReader,
    ) {}

    /**
     * The state at exactly `$version`, for a caller that observed the stream head at or past it.
     *
     * A history that ends before `$version` is therefore truncated rather than young, and refused
     * as corruption instead of being served as a partial state.
     *
     * @throws InspectionVersionOutOfRange when `$version` is not positive or exceeds `MAX_VERSIONS`
     * @throws AggregateTypeMismatch when `$id` is not an instance of the bound identity class
     * @throws CorruptAggregateHistory when the history stops before `$version` or the replay
     *                                 contradicts its own version headers
     * @throws UnknownEventType when a stored event type resolves to no known event class
     * @throws StorageFailure on a store read or deserialization failure
     */
    public function stateAt(AggregateIdentity $id, int $version): HistoricalAggregateState
    {
        if ($version > self::MAX_VERSIONS) {
            throw InspectionVersionOutOfRange::pastCeiling($this->aggregateClass, $version, self::MAX_VERSIONS);
        }

        $state = $this->foldUpTo($id, $version);

        if ($state === null || $state->version !== $version) {
            throw CorruptStreamHistory::incompleteHistory($this->aggregateClass, $id->toString(), $version, $state === null ? 0 : $state->version);
        }

        return $state;
    }

    /**
     * The fold of whatever the stream holds up to `$version`: null for an empty stream, and the
     * version actually reached when the stream is shorter.
     *
     * For a caller that owns its own verdict on a short history, such as a snapshot verifier
     * comparing a claimed version with what the stream still holds.
     *
     * @throws InspectionVersionOutOfRange when `$version` is not positive
     * @throws AggregateTypeMismatch when `$id` is not an instance of the bound identity class
     * @throws CorruptAggregateHistory when the replay contradicts its own version headers
     * @throws UnknownEventType when a stored event type resolves to no known event class
     * @throws StorageFailure on a store read or deserialization failure
     */
    public function foldUpTo(AggregateIdentity $id, int $version): ?HistoricalAggregateState
    {
        if ($version < 1) {
            throw InspectionVersionOutOfRange::notPositive($this->aggregateClass, $version);
        }

        // the same runtime keeper the repositories carry: the template only exists in phpdoc, and
        // a foreign identity whose string collides would otherwise read another aggregate's stream
        if ($id::class !== $this->idClass) {
            throw AggregateTypeMismatch::id($this->idClass, $id::class);
        }

        $stream = new StreamName($this->category->value)->withQualifier($id->toString())->toString();

        // the read's infrastructure failures cross as the contracted StorageFailure, the translation
        // the repository applies; the replay's own corruption and an unknown type stay distinct
        try {
            $aggregate = ($this->aggregateClass)::reconstitute(
                $id,
                AggregateHistoryReplay::validated(
                    $this->aggregateClass,
                    $id,
                    $this->streamReader->retrieveByFilter(new AggregateStreamUpTo($this->category, $stream, $version)),
                    0,
                ),
            );
        } catch (Exception|InvalidPosition|SerializationExceptionContract|ClockExceptionContract|NotADomainEvent $e) {
            throw StorageFailure::wrap(sprintf('the %s historical read', $this->category), $e);
        }

        return $aggregate === null ? null : new HistoricalAggregateState($aggregate->version(), $aggregate->toSnapshot());
    }
}
