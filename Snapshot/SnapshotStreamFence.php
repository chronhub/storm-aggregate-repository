<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Snapshot;

use Closure;
use Storm\AggregateRepository\Console\SnapshotSweepCommand;
use Storm\AggregateRepository\Exception\SnapshotFenceRefused;
use Throwable;

/**
 * Mutual exclusion, per stream, between the snapshot sweep of that stream and its erase.
 *
 * A sweep reads the head, replays the events, then writes a snapshot; an erase deletes the snapshot,
 * then the events and the head. Interleaved, the sweep writes back the snapshot of a life the erase
 * has already removed, and an aggregate later recreated under the same id is served that dead state.
 * The fence orders the two, and orders nothing else: appends to the same stream never touch it, and
 * unrelated streams use distinct lock inputs; adapter hash collisions may add contention.
 *
 * Three adapter laws:
 *
 * - Try-skip. The fence never blocks. An occupied stream answers `false`, `$work` does not run, and
 *   the caller decides what a refusal means; a waiting fence would let one side hold the stream while
 *   queueing behind the other.
 *
 * - One atomic unit. `$work` runs inside the unit that carries the exclusion, so everything it wrote
 *   commits or rolls back with the fence, and the exclusion lasts exactly until that unit settles;
 *   never released before its commit, never held past it.
 *
 * - `READ COMMITTED`, verified against the server before `$work` runs, never assumed from local
 *   bookkeeping. A replay and a head probe are plain reads, and only a per-statement snapshot lets an
 *   event committed a moment ago be seen.
 *
 * The two verbs differ on one thing, who owns the transaction, and that is what separates the two
 * callers: a sweep must bound how long it can make an erase wait, an erase must be able to live
 * inside the caller's own unit of work.
 *
 * @see SnapshotSweepCommand the bounded caller
 * @see SnapshotDeletingStreamEraser the joining caller
 */
interface SnapshotStreamFence
{
    /**
     * Run `$work` holding the fence for `$stream`, inside a transaction the fence owns and bounds.
     *
     * For the caller that must not starve the other side. A sweep replays a whole aggregate, and
     * every erase of that stream is refused while it holds the fence, so the hold carries an explicit
     * ceiling instead of running for as long as the replay happens to take. The ceiling is the
     * transaction's total duration, not one statement's, and reaching it is a failure of this call,
     * not a silent truncation of the work.
     *
     * An already open transaction is refused rather than joined; its throws clause says why.
     *
     * @param  Closure():void  $work
     * @return bool whether the fence was taken and `$work` ran
     *
     * @throws SnapshotFenceRefused when a transaction is already open on the connection, or when the
     *                              effective isolation level is not `READ COMMITTED`; both before `$work` runs
     * @throws Throwable propagated from `$work`, which is arbitrary here and unnameable at this port,
     *                   or raised by the fence's own transaction, the expiry of the bound included;
     *                   the fence is released either way
     */
    public function tryBounded(string $stream, Closure $work): bool;

    /**
     * Run `$work` holding the fence for `$stream`, joining the caller's transaction when there is one.
     *
     * For the caller whose own unit of work has to cover `$work`. An erase deletes the snapshot and
     * the stream together, and an application wrapping that erase in a wider transaction keeps both
     * halves under its own commit. The exclusion then lasts until the outer commit, which is what the
     * erase needs: a sweep must not be able to snapshot the stream between the delete and that
     * commit.
     *
     * Nothing here bounds the hold, since the transaction may be the caller's and its lifetime is not
     * this port's to change.
     *
     * @param  Closure():void  $work
     * @return bool whether the fence was taken and `$work` ran
     *
     * @throws SnapshotFenceRefused when the effective isolation level is not `READ COMMITTED`, before
     *                              `$work` runs
     * @throws Throwable propagated from `$work`, which is arbitrary here and unnameable at this port,
     *                   or raised by the transaction the fence joined or opened
     */
    public function tryWithin(string $stream, Closure $work): bool;
}
