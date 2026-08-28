<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Snapshot;

use InvalidArgumentException;
use Storm\AggregateRepository\SnapshotRepository;
use Storm\Clock\PointInTime;

/**
 * A stored snapshot: the cached state of one aggregate at a version. An infrastructure envelope
 * around the domain's array state that never crosses into the domain; the `SnapshotRepository`
 * decorator unpacks it before calling `fromSnapshot()`. Latest-only: one row per stream.
 *
 * State carries the aggregate's `_snapshot_version` marker set by `toSnapshot()`; a stale marker is
 * discarded on reconstruction.
 *
 * The constructor holds the storage-independent invariants: a usable snapshot names a stream and a
 * type, sits at a positive version, and carries an associative state bag. The writer only ever
 * snapshots a retrieved aggregate, so the version is at least 1 by contract. The DBAL store filters
 * a corrupt ROW into a cache miss at load; this gate keeps a corrupt VALUE from ever being handed to
 * `save()` in the first place.
 *
 * @see SnapshotRepository
 * @see \Storm\Contracts\Aggregate\SnapshotableAggregateRoot
 */
final readonly class Snapshot
{
    /**
     * @param  string  $stream  the qualified stream, for example `account-123`; the store key
     * @param  string  $aggregateType  the aggregate FQCN, for diagnostics and the sweep
     * @param  int  $version  the aggregate version this snapshot was taken at, at least 1
     * @param  array<string, mixed>  $state  the array returned by `toSnapshot()`
     * @param  PointInTime  $createdAt  when the snapshot was taken, read by the time-based sweep
     *
     * @throws InvalidArgumentException when the stream or type is blank, the version is below 1, or
     *                                  the state is not an associative bag
     */
    public function __construct(
        public string $stream,
        public string $aggregateType,
        public int $version,
        public array $state,
        public PointInTime $createdAt,
    ) {
        if (trim($stream) === '' || trim($aggregateType) === '') {
            throw new InvalidArgumentException('A snapshot names its stream and its aggregate type — blank keys cannot be stored nor matched back.');
        }

        if ($version < 1) {
            throw new InvalidArgumentException(sprintf('A snapshot is only ever taken at version >= 1, got %d — the writer snapshots a retrieved aggregate, never a phantom.', $version));
        }

        if ($state !== [] && array_is_list($state)) { // @phpstan-ignore function.impossibleType (the phpdoc shape is exactly what this guard defends at runtime)
            throw new InvalidArgumentException('A snapshot state is the associative bag returned by toSnapshot() — a list would restore garbage keys.');
        }
    }
}
