<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Snapshot;

use Doctrine\DBAL\Connection;
use InvalidArgumentException;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use ReflectionParameter;
use Storm\AggregateRepository\Snapshot\PgSnapshotStreamFence;

/**
 * The bound's construction alone: the fence's lock and transaction are proven against a real
 * PostgreSQL by the integration suite, which a unit test driving them would duplicate.
 */
final class PgSnapshotStreamFenceTest extends TestCase
{
    #[Test]
    public function the_default_bound_is_thirty_seconds(): void
    {
        $this->assertSame(30_000, new ReflectionParameter([PgSnapshotStreamFence::class, '__construct'], 'boundMs')->getDefaultValue());
    }

    #[Test]
    public function a_one_millisecond_bound_is_a_valid_bound(): void
    {
        $this->expectNotToPerformAssertions();

        new PgSnapshotStreamFence($this->createStub(Connection::class), 1);
    }

    #[Test]
    public function a_zero_bound_is_refused_since_postgres_reads_it_as_no_bound_at_all(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('got 0.');

        new PgSnapshotStreamFence($this->createStub(Connection::class), 0);
    }

    #[Test]
    public function a_negative_bound_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('got -1.');

        new PgSnapshotStreamFence($this->createStub(Connection::class), -1);
    }
}
