<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Exception;

use RuntimeException;
use Storm\Contracts\Aggregate\CorruptAggregateHistory as CorruptAggregateHistoryContract;

/**
 * Thrown when the stream records themselves contradict the event-sourcing invariants.
 *
 * Detected at the repository, the only place the raw `EventRecord` coordinates are still visible
 * before the records are reduced to pure domain events:
 *
 * - Versions must run contiguously, from 1 on a full replay or from the snapshot version plus one on a tail
 * - Every record must carry its version header
 * - Every event's own aggregate id must name the stream's aggregate
 *
 * A violation is a data-integrity alarm, whether a truncated read, a foreign row or a record with no
 * version header, never state to replay: a decision on it could pass the store's CAS and become a
 * durable fact.
 *
 * Implements the same contracted corruption as the trait's own exactness check, so a caller
 * catching the `CorruptAggregateHistory` contract sees one failure family for the whole replay
 * path.
 */
final class CorruptStreamHistory extends RuntimeException implements CorruptAggregateHistoryContract
{
    public static function brokenContinuity(string $aggregateClass, string $aggregateId, int $expected, int $observed): self
    {
        return new self(sprintf(
            'The stream of %s (id %s) is not contiguous: expected version %d, observed %d — a gap, duplicate or out-of-order record; a replay of it would decide on a state the stream disproves.',
            $aggregateClass,
            $aggregateId,
            $expected,
            $observed,
        ));
    }

    public static function missingVersionHeader(string $aggregateClass, string $aggregateId, string $eventClass, int $expected): self
    {
        return new self(sprintf(
            'The stream of %s (id %s) holds a record (%s) with no aggregate version header where version %d was expected; an unheadered record cannot participate in the OCC token and its state cannot be trusted.',
            $aggregateClass,
            $aggregateId,
            $eventClass,
            $expected,
        ));
    }

    public static function foreignEvent(string $aggregateClass, string $aggregateId, string $eventClass, string $observedId): self
    {
        return new self(sprintf(
            'The stream of %s (id %s) holds a record (%s) whose own aggregate id is "%s" — a foreign or imported row; applying it would silently launder another aggregate\'s state into this one.',
            $aggregateClass,
            $aggregateId,
            $eventClass,
            $observedId,
        ));
    }
}
