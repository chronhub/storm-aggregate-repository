<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Storm\AggregateRepository\Snapshot\DbalSnapshotStore;
use Storm\AggregateRepository\SnapshotRepository;

/**
 * `load()` treats an unreadable snapshot row as a cache miss and returns null, never an error: a
 * snapshot is only a cache, and {@see SnapshotRepository} replays from the authoritative events
 * whenever load returns null. There is nothing a caller could do with a corrupt row but replay,
 * which null already triggers, so the read swallows the corruption instead of leaking it across the
 * port.
 *
 * PostgreSQL rejects malformed JSON and timestamps, so those defensive branches use row doubles.
 * Valid JSON with an invalid state shape remains reachable through the schema. Transaction recovery
 * after failed cleanup is verified against PostgreSQL in the integration suite.
 */
final class DbalSnapshotStoreTest extends TestCase
{
    #[Test]
    public function a_corrupt_state_blob_reads_as_a_cache_miss(): void
    {
        // malformed JSON in `state`: json_decode throws, is swallowed, so a miss replaying from events.
        $store = new DbalSnapshotStore($this->connectionReturning([
            'stream' => 'article-1',
            'aggregate_type' => 'App\\Article',
            'version' => 5,
            'state' => '{not valid json',
            'created_at' => '2024-01-01 10:00:00.000000+00',
        ]));

        self::assertNull($store->load('article-1'));
    }

    #[Test]
    public function an_unparseable_created_at_reads_as_a_cache_miss(): void
    {
        // a created_at PointInTime::fromStorage cannot parse: InvalidDateTimeException, swallowed, so a miss.
        $store = new DbalSnapshotStore($this->connectionReturning([
            'stream' => 'article-1',
            'aggregate_type' => 'App\\Article',
            'version' => 5,
            'state' => '{"title":"ok"}',
            'created_at' => 'not-a-timestamp',
        ]));

        self::assertNull($store->load('article-1'));
    }

    #[Test]
    public function a_corrupt_row_is_deleted_on_sight_so_a_same_version_save_can_regenerate_it(): void
    {
        // a silent skip would full-replay on EVERY read forever: save()'s monotonic guard refuses to
        // overwrite at the same version, so deletion is what makes the cache self-healing for real
        $connection = $this->createMock(Connection::class);
        $connection->method('fetchAssociative')->willReturn([
            'stream' => 'article-1',
            'aggregate_type' => 'App\\Article',
            'version' => 5,
            'state' => '123', // a scalar jsonb, corrupt shape
            'created_at' => '2024-01-01 10:00:00.000000+00',
        ]);
        $connection->expects($this->once())
            ->method('executeStatement')
            ->with($this->stringContains('DELETE FROM snapshots'), ['stream' => 'article-1'])
            ->willReturn(1);

        self::assertNull(new DbalSnapshotStore($connection)->load('article-1'));
    }

    #[Test]
    public function a_failed_discard_still_reads_as_a_cache_miss(): void
    {
        // This double checks cleanup failure without a caller transaction. PostgreSQL integration
        // tests exercise recovery of an active transaction before the authoritative replay.
        $connection = $this->createStub(Connection::class);
        $connection->method('fetchAssociative')->willReturn([
            'stream' => 'article-1',
            'aggregate_type' => 'App\\Article',
            'version' => 5,
            'state' => '123', // a scalar jsonb, corrupt shape
            'created_at' => '2024-01-01 10:00:00.000000+00',
        ]);
        $connection->method('executeStatement')->willThrowException(
            new class('cannot execute DELETE in a read-only transaction') extends RuntimeException implements DbalException {},
        );

        self::assertNull(new DbalSnapshotStore($connection)->load('article-1'));
    }

    /**
     * @param  array<string, mixed>  $row
     */
    private function connectionReturning(array $row): Connection
    {
        $connection = $this->createStub(Connection::class);
        $connection->method('fetchAssociative')->willReturn($row);
        $connection->method('executeStatement')->willReturn(1); // the corrupt arms delete the row

        return $connection;
    }

    #[Test]
    public function a_scalar_json_state_reads_as_a_cache_miss(): void
    {
        // jsonb happily stores `123`; fed to Snapshot(array $state) a scalar would escape as a raw
        // TypeError OUTSIDE the JsonException catch, so it takes the same doctrine as malformed JSON: a miss.
        $store = new DbalSnapshotStore($this->connectionReturning([
            'stream' => 'article-1',
            'aggregate_type' => 'App\\Article',
            'version' => 5,
            'state' => '123',
            'created_at' => '2024-01-01 10:00:00.000000+00',
        ]));

        self::assertNull($store->load('article-1'));
    }

    #[Test]
    public function a_json_list_state_reads_as_a_cache_miss(): void
    {
        // a list passes the array type but is not a state bag; restoreState would coerce garbage.
        $store = new DbalSnapshotStore($this->connectionReturning([
            'stream' => 'article-1',
            'aggregate_type' => 'App\\Article',
            'version' => 5,
            'state' => '[1,2]',
            'created_at' => '2024-01-01 10:00:00.000000+00',
        ]));

        self::assertNull($store->load('article-1'));
    }

    #[Test]
    public function a_non_positive_version_reads_as_a_cache_miss(): void
    {
        // the writer only ever snapshots at version >= 1 by the monotonic SQL guard; a row claiming less
        // is corrupt cache, discarded here at the source rather than clamped into a usable aggregate.
        $store = new DbalSnapshotStore($this->connectionReturning([
            'stream' => 'article-1',
            'aggregate_type' => 'App\\Article',
            'version' => 0,
            'state' => '{"title":"ok"}',
            'created_at' => '2024-01-01 10:00:00.000000+00',
        ]));

        self::assertNull($store->load('article-1'));
    }
}
