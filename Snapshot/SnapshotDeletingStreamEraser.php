<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Snapshot;

use Storm\AggregateRepository\Console\SnapshotPruneOrphansCommand;
use Storm\Chronicler\Erasure\StreamEraser;
use Storm\Stream\StreamName;
use Throwable;

/**
 * The snapshot-coherence half of an erase: deletes the stream's snapshot row, THEN erases the
 * stream. Wired by the bundle as a decoration of the `StreamEraser` port; Chronicler keeps not
 * knowing snapshots exist, the bundle sees both ports, the same composition as the existence-cache
 * eviction decorator.
 *
 * Without it, an erased stream leaves its snapshot behind and three failures follow:
 *
 * - The ORPHAN: pure cache resurrecting an aggregate the store disproves, though this is guarded at
 *   read.
 *
 * - The RECREATION: the same id re-created starts at version 1 while the old snapshot sits ahead; no
 *   longer an orphan, it is served as the old life.
 *
 * - The MIXING: the new life grows past the snapshot version and its events replay ONTO the old
 *   state, silently; undetectable at read, even against the head.
 *
 * Deleting the snapshot at the erase is the only closure that covers all three.
 *
 * Snapshot FIRST, deliberately: if the erase then fails, the stream lives on without its cache;
 * harmless, the next sweep regenerates it. The reverse order would leave a crash window where the
 * stream is gone but its snapshot survives, re-arming all three failures until the prune. The
 * at-least-once leftovers of a crash between the two statements are covered by the read guard and
 * `storm:snapshot:prune-orphans`, both backstops, not the mechanism.
 *
 * @see \Storm\Chronicler\Erasure\StreamEraser the decorated port, whose contract deliberately scopes snapshots out
 * @see SnapshotPruneOrphansCommand the backstop for crash leftovers
 */
final readonly class SnapshotDeletingStreamEraser implements StreamEraser
{
    public function __construct(
        private StreamEraser $eraser,
        private SnapshotStore $snapshots,
    ) {}

    /**
     * {@inheritDoc}
     *
     * @throws Throwable on a storage failure of the snapshot deletion or of the erase itself
     */
    public function erase(StreamName $streamName): int
    {
        $this->snapshots->delete($streamName->toString());

        return $this->eraser->erase($streamName);
    }
}
