<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Exception;

use InvalidArgumentException;
use Storm\Contracts\Aggregate\EventAggregateIdMismatch as EventAggregateIdMismatchContract;

/**
 * Thrown at `store()` when a released event's own `aggregateId()` contradicts the identity of the
 * aggregate being stored. A domain bug, typically an event built with another aggregate's id from a
 * copy-paste in a factory, that would otherwise persist a permanently incoherent fact: the stream
 * names one aggregate, the payload another, and the divergence surfaces only in whichever consumer
 * reads the other source.
 */
final class EventAggregateIdMismatch extends InvalidArgumentException implements EventAggregateIdMismatchContract
{
    public static function of(string $eventClass, string $expected, string $actual): self
    {
        return new self(sprintf(
            '%s claims aggregate id "%s" but is being stored under "%s" — the event payload id must match the aggregate identity.',
            $eventClass,
            $actual,
            $expected,
        ));
    }
}
