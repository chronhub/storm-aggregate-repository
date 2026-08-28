<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Schema;

/**
 * Raw PostgreSQL DDL for `snapshots`, the aggregate snapshot cache.
 *
 * One row per stream, latest-only:
 *
 * - `version` is the aggregate version the state was captured at.
 *
 * - `state` is the jsonb blob from `toSnapshot()`.
 *
 * - `created_at` is when it was taken, read by the time-based sweep.
 *
 * `PRIMARY KEY (stream)` matches `stream_heads.stream` in TYPE and in COLLATION, so the sweep's
 * `stream_heads` join with `snapshots` is index-optimal. The `COLLATE "C"` is half of a pair and
 * carries no meaning alone: a comparison between one pinned side and one default side resolves to
 * the pinned collation, and an index built under the other one can no longer serve it. Measured, the
 * mismatch turns the per-row probe into a full scan of `snapshots`, whose cost then follows the size
 * of the whole store rather than the batch asked for. Unpin one side only by unpinning both. Idempotent via `IF NOT EXISTS`; Ledger aggregates it into
 * `storm:install`.
 *
 * Deliberately NO foreign key to `stream_heads`; the topological option was weighed and not taken.
 * An `ON DELETE CASCADE` would be the most atomic invalidation, but it couples this package's DDL
 * to another package's table, both separately installable subtree splits, and graves "same database
 * forever" into the schema while `storm.connections` is built to grow. Coherence is composed
 * instead: the bundle decorates `StreamEraser` to delete the snapshot at the erase, the read guard
 * probes the stream head on an empty tail, and `prune-orphans` backstops.
 */
final class SnapshotSchema
{
    /**
     * @return list<string>
     */
    public static function up(): array
    {
        return [
            /** @lang PostgreSQL */
            <<<'SQL'
                CREATE TABLE IF NOT EXISTS snapshots (
                    stream         text           COLLATE "C" NOT NULL,
                    aggregate_type text           NOT NULL,
                    version        bigint         NOT NULL,
                    state          jsonb          NOT NULL,
                    created_at     timestamptz(6) NOT NULL DEFAULT clock_timestamp(),
                    CONSTRAINT snapshots_pk PRIMARY KEY (stream)
                )
                SQL,
        ];
    }

    /**
     * @return list<string>
     */
    public static function down(): array
    {
        return [
            /** @lang PostgreSQL */
            'DROP TABLE IF EXISTS snapshots',
        ];
    }
}
