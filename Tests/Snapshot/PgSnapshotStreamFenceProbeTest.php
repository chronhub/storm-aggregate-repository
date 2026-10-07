<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Snapshot;

use Doctrine\DBAL\Connection;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Storm\AggregateRepository\Snapshot\PgSnapshotStreamFence;

/**
 * The liveness probe that follows a failed bounded transaction, over a stubbed connection.
 *
 * A real PostgreSQL cannot refuse the probe on demand: DBAL already closes a handle whose loss it
 * recognizes, so the probe reconnects and passes. The stub stands for the case it cannot show, a
 * probe that fails because no new session can be opened.
 */
final class PgSnapshotStreamFenceProbeTest extends TestCase
{
    #[Test]
    public function a_probe_that_fails_drops_the_handle_and_the_original_failure_reaches_the_caller(): void
    {
        $failure = new RuntimeException('transaction timeout');
        $connection = $this->createMock(Connection::class);
        $connection->method('transactional')->willThrowException($failure);
        $connection->expects(self::once())->method('executeStatement')->with('SELECT 1')->willThrowException(new RuntimeException('server unreachable'));
        $connection->expects(self::once())->method('close');

        $this->assertReachesTheCaller($failure, $connection);
    }

    #[Test]
    public function a_probe_that_passes_keeps_the_healthy_session(): void
    {
        // an ordinary failure must not cost the session its search path and settings
        $failure = new RuntimeException('replay failed');
        $connection = $this->createMock(Connection::class);
        $connection->method('transactional')->willThrowException($failure);
        $connection->expects(self::once())->method('executeStatement')->with('SELECT 1')->willReturn(1);
        $connection->expects(self::never())->method('close');

        $this->assertReachesTheCaller($failure, $connection);
    }

    private function assertReachesTheCaller(RuntimeException $failure, Connection $connection): void
    {
        try {
            new PgSnapshotStreamFence($connection, 1_000)->tryBounded('article-1', static function (): void {});
            self::fail('the failure of the owned transaction must reach the caller');
        } catch (RuntimeException $caught) {
            self::assertSame($failure, $caught);
        }
    }
}
