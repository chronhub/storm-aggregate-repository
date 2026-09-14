<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Snapshot;

use Storm\AggregateRepository\Console\SnapshotPruneOrphansCommand;
use Storm\AggregateRepository\Exception\SnapshotFenceRefused;
use Storm\AggregateRepository\Exception\SnapshotStreamBusy;
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
 * The snapshot deletion and delegated erase share one transaction. Both commit or both roll back,
 * so an erase failure preserves the existing cache. Pruning remains a backstop for leftovers from
 * maintenance paths that do not participate in this protocol.
 *
 * Atomicity covers a crash; exclusion also protects concurrent production. Both halves run holding
 * the stream's `SnapshotStreamFence`, which the sweep holds across replay and save. A sweep cannot
 * republish an old life after the participating erase commits, and appends remain unchanged.
 *
 * A busy stream raises `SnapshotStreamBusy` before this call deletes anything. The caller retries
 * after the competing snapshot operation finishes, including an erase held by an outer transaction.
 *
 * @see \Storm\Chronicler\Erasure\StreamEraser the decorated port, whose contract deliberately scopes snapshots out
 * @see SnapshotPruneOrphansCommand the backstop for crash leftovers
 */
final readonly class SnapshotDeletingStreamEraser implements StreamEraser
{
    public function __construct(
        private StreamEraser $eraser,
        private SnapshotStore $snapshots,
        private SnapshotStreamFence $fence,
    ) {}

    /**
     * {@inheritDoc}
     *
     * The fence joins an ambient transaction when the application wraps this erase in one, so the
     * snapshot delete and the erase settle under the caller's commit and the stream stays fenced
     * until then. A failure of either half propagates rather than being absorbed, so the rollback
     * that follows covers the snapshot delete as well; absorbing it would commit a stream whose
     * snapshot is gone and whose events are not.
     *
     * @throws SnapshotStreamBusy when another snapshot operation holds the stream, before anything is deleted
     * @throws SnapshotFenceRefused when the effective isolation level is not `READ COMMITTED`
     * @throws Throwable on a storage failure of the snapshot deletion or of the erase itself
     */
    public function erase(StreamName $streamName): int
    {
        $stream = $streamName->toString();
        $erased = 0;

        $held = $this->fence->tryWithin($stream, function () use ($stream, $streamName, &$erased): void {
            $this->snapshots->delete($stream);

            $erased = $this->eraser->erase($streamName);
        });

        if (! $held) {
            throw SnapshotStreamBusy::sweptConcurrently($stream);
        }

        return $erased;
    }
}
