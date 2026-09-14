<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Exception;

use RuntimeException;
use Storm\AggregateRepository\Snapshot\SnapshotDeletingStreamEraser;
use Storm\Contracts\Aggregate\AggregateExceptionContract;

/**
 * An erase was refused because another snapshot operation holds the stream.
 *
 * This call has deleted nothing. Retry after the competing operation finishes; an erase inside an
 * application transaction retains its lock until that transaction ends.
 *
 * @see SnapshotDeletingStreamEraser
 */
final class SnapshotStreamBusy extends RuntimeException implements AggregateExceptionContract
{
    private function __construct(
        /** The stream whose erase was refused, so a caller retries or reports it without parsing the message. */
        public readonly string $stream,
        string $message,
    ) {
        parent::__construct($message);
    }

    public static function sweptConcurrently(string $stream): self
    {
        return new self($stream, sprintf(
            'Erasing stream "%s" was refused because another snapshot operation holds it. '
            .'This erase deleted nothing; retry after the competing operation and its transaction finish.',
            $stream,
        ));
    }
}
