<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Exception;

use LogicException;
use Storm\AggregateRepository\Snapshot\SnapshotStreamFence;
use Storm\Contracts\Aggregate\AggregateExceptionContract;

/**
 * The snapshot fence refused a stream before running any work, on a condition that comes from wiring
 * and that a retry cannot change.
 *
 * Two conditions, both read from the server rather than assumed:
 *
 * - The effective isolation level is not `READ COMMITTED`. A sweep replays events and probes the
 *   stream head with plain reads, and only a per-statement snapshot lets an event committed a moment
 *   ago be seen. Under `REPEATABLE READ` or `SERIALIZABLE` that miss is silent, no serialization
 *   failure ever names it, so the level is refused instead of argued about.
 *
 * - A bounded hold was asked for on a connection that already has a transaction open. The bound
 *   belongs to the transaction, so joining the caller's would re-time it and end it from underneath,
 *   destroying work the caller never agreed to bound.
 *
 * @see SnapshotStreamFence
 */
final class SnapshotFenceRefused extends LogicException implements AggregateExceptionContract
{
    public static function boundedUnderAmbientTransaction(string $stream): self
    {
        return new self(sprintf(
            'The bounded snapshot fence for stream "%s" was asked to run inside a transaction that was already '
            .'open on its connection. It sets a total duration on the transaction it owns, and applying that to '
            .'the caller\'s would end the caller\'s transaction at the bound. Close the ambient transaction '
            .'before sweeping, or give the sweep its own connection.',
            $stream,
        ));
    }

    public static function isolationUnderOwnedTransaction(string $stream, string $effectiveLevel): self
    {
        return new self(sprintf(
            'The snapshot fence opened its own transaction for stream "%s" and found it at isolation level "%s"; '
            .'READ COMMITTED is required for the replay and the head probe to see a freshly committed event. '
            .'DBAL tracks the level it set itself, so this one was set behind it, by raw SQL or a pooler '
            .'default; reset the session default to READ COMMITTED.',
            $stream,
            $effectiveLevel,
        ));
    }

    public static function isolationUnderAmbientTransaction(string $stream, string $effectiveLevel): self
    {
        return new self(sprintf(
            'The snapshot fence for stream "%s" runs under an ambient transaction at isolation level "%s"; '
            .'READ COMMITTED is required for the replay and the head probe to see a freshly committed event. '
            .'The level belongs to the caller that opened the transaction, or to a pooler or raw SQL that set '
            .'the session default; open that transaction READ COMMITTED or let the fence own its own.',
            $stream,
            $effectiveLevel,
        ));
    }
}
