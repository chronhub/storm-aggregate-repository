<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Snapshot;

use PHPUnit\Framework\TestCase;
use Storm\AggregateRepository\Exception\SnapshotFenceRefused;
use Storm\AggregateRepository\Exception\SnapshotStreamBusy;
use Storm\Contracts\Aggregate\AggregateExceptionContract;

final class SnapshotExceptionContractTest extends TestCase
{
    public function test_fence_refusals_carry_the_aggregate_exception_contract(): void
    {
        foreach ([
            SnapshotFenceRefused::boundedUnderAmbientTransaction('account-1'),
            SnapshotFenceRefused::isolationUnderOwnedTransaction('account-1', 'repeatable read'),
            SnapshotFenceRefused::isolationUnderAmbientTransaction('account-1', 'serializable'),
        ] as $error) {
            self::assertInstanceOf(AggregateExceptionContract::class, $error);
        }
    }

    public function test_busy_streams_carry_the_aggregate_exception_contract(): void
    {
        self::assertInstanceOf(AggregateExceptionContract::class, SnapshotStreamBusy::sweptConcurrently('account-1'));
    }
}
