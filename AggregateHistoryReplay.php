<?php

declare(strict_types=1);

namespace Storm\AggregateRepository;

use Doctrine\DBAL\Exception;
use Generator;
use Storm\AggregateRepository\Exception\CorruptStreamHistory;
use Storm\Chronicler\Exception\NotADomainEvent;
use Storm\Chronicler\Record\EventRecord;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Chronicler\InvalidPosition;
use Storm\Contracts\Chronicler\UnknownEventType;
use Storm\Contracts\Clock\ClockExceptionContract;
use Storm\Contracts\Message\DomainEvent;
use Storm\Contracts\Serializer\SerializationExceptionContract;

/**
 * The single event-sourcing replay guard, shared by the full reconstitution and the snapshot tail.
 *
 * Both read paths must enforce the same invariants while the raw record coordinates are still
 * visible, since everything downstream sees only `DomainEvent`s and one final version integer: this
 * is the last place a gap, a non-version header record or a foreign row can be told apart. The two
 * callers differ only in where the count STARTS and in HOW the records were read; `$fromVersion` is
 * 0 for the full replay, the snapshot version for the tail. The read stays at the call site, so this
 * holds no state and takes the record stream as an argument, never reaching for a store.
 *
 * @see DefaultAggregateRepository
 * @see SnapshotRepository
 */
final class AggregateHistoryReplay
{
    /**
     * Re-yield the records as pure domain events, oldest first, asserting the event-sourcing invariants.
     *
     * - Versions run contiguously from `$fromVersion` + 1
     * - Every record carries its version header
     * - Every event's own aggregate id names the requested aggregate, the mirror of the write-side check
     *
     * The Generator's return value is the reached version, which the aggregate's reconstitution
     * re-checks for exactness against its applied count.
     *
     * @param  class-string  $aggregateClass  the bound aggregate, named in the corruption reports
     * @param  Generator<int, EventRecord>  $records
     * @return Generator<int, DomainEvent, null, int>
     *
     * @throws CorruptStreamHistory when the stream is not contiguous from $fromVersion, a record has
     *                              no version header, or an event belongs to another aggregate
     * @throws NotADomainEvent when a stored message is not a domain event
     * @throws UnknownEventType when a stored event type alias maps to no registered class
     * @throws SerializationExceptionContract when a stored payload cannot be deserialized
     * @throws InvalidPosition when a stored position is not a positive integer
     * @throws ClockExceptionContract when a stored point in time failed to be parsed
     * @throws Exception on a DBAL failure of the underlying read
     */
    public static function validated(string $aggregateClass, AggregateIdentity $id, Generator $records, int $fromVersion): Generator
    {
        $version = $fromVersion;

        foreach ($records as $record) {
            $event = $record->event();
            $header = $record->message->aggregateVersion();

            if ($header === null) {
                throw CorruptStreamHistory::missingVersionHeader($aggregateClass, $id->toString(), $event::class, $version + 1);
            }

            if ($header !== $version + 1) {
                throw CorruptStreamHistory::brokenContinuity($aggregateClass, $id->toString(), $version + 1, $header);
            }

            if ($event->aggregateId() !== $id->toString()) {
                throw CorruptStreamHistory::foreignEvent($aggregateClass, $id->toString(), $event::class, $event->aggregateId());
            }

            $version = $header;

            yield $event;
        }

        return $version;
    }
}
