<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Snapshot;

use Throwable;

/**
 * Persists and loads aggregate snapshots. Latest-only: one snapshot per stream; a snapshot is a
 * cache, never history, so older states are not kept. It exists purely to speed up `retrieve()`: load
 * snapshot at N, then replay only the events after N instead of the whole stream; the events remain the
 * source of truth.
 *
 * Lives in the package rather than Contracts: it names the concrete `Snapshot` DTO and is infrastructure DBAL.
 */
interface SnapshotStore
{
    /**
     * The latest snapshot for a stream, or null when none exists.
     *
     * @throws Throwable on a storage failure
     */
    public function load(string $stream): ?Snapshot;

    /**
     * Store the snapshot for its stream, upsert on stream. Implementations MUST be monotonic, never
     * replacing a stored snapshot with a lower version, so a stale or overlapping save cannot make the
     * cache regress.
     *
     * @throws Throwable on a storage failure, or when the state cannot be encoded
     */
    public function save(Snapshot $snapshot): void;

    /**
     * Remove a stream's snapshot. Returns whether a row was deleted.
     *
     * @throws Throwable on storage failure
     */
    public function delete(string $stream): bool;

    /**
     * Streams due for a snapshot, found set-based. A stream's drift `d` is its head version minus its
     * last snapshot version, a missing snapshot counting as 0. Due when EITHER:
     *
     * - The count trigger fires when `d >= threshold`, unless throttled by a set `minInterval` with the
     *   snapshot younger than that.
     *
     * - The staleness ceiling fires: the aggregate changed, `d >= 1`, and its snapshot is older than
     *   `maxAge`.
     *
     * A null `maxAge` / `minInterval` disables that clause, leaving the pure count trigger. The `d >= 1`
     * guard keeps the age clause from re-snapshotting an unchanged aggregate forever; a missing snapshot
     * keeps the throttle from ever blocking a first snapshot. `batch` caps the streams returned per page.
     *
     * `$after` is the keyset cursor: only streams sorting strictly after it are returned, so a caller
     * that failed some of a page can request the NEXT page instead of being handed the same sorted
     * prefix forever; a persistent poison must never starve everything sorted behind it.
     *
     * @return list<string> sorted by stream, ascending; the cursor's contract
     *
     * @throws Throwable on a storage failure
     */
    public function staleStreams(string $category, int $threshold, ?int $maxAge, ?int $minInterval, int $batch, ?string $after = null): array;

    /**
     * Count the orphan snapshots, rows whose stream no longer exists in `stream_heads`; the dry-run
     * preview of `pruneOrphans()`.
     *
     * @throws Throwable on a storage failure
     */
    public function countOrphans(): int;

    /**
     * Prune the orphan snapshots in `$batch`-capped statements, never a long lock. Structural, not
     * age-based: the guard is "no matching stream_heads row", so it can only ever remove a snapshot whose
     * source stream is gone, never a live one.
     *
     * @param  positive-int  $batch
     * @return int snapshots pruned
     *
     * @throws Throwable on a storage failure
     */
    public function pruneOrphans(int $batch): int;
}
