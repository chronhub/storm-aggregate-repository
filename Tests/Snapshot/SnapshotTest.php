<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Snapshot;

use InvalidArgumentException;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Storm\AggregateRepository\Snapshot\Snapshot;
use Storm\Clock\PointInTime;

final class SnapshotTest extends TestCase
{
    #[Test]
    public function carries_a_valid_snapshot(): void
    {
        $snapshot = new Snapshot('article-1', 'App\\Article', 5, ['title' => 'ok'], PointInTime::from('2024-01-01T10:00:00.000000+00:00'));

        $this->assertSame(5, $snapshot->version);
    }

    #[Test]
    #[Group('adversarial')]
    public function refuses_a_non_positive_version(): void
    {
        // the writer snapshots a retrieved aggregate, >= 1 by contract; anything else is a phantom
        $this->expectException(InvalidArgumentException::class);

        new Snapshot('article-1', 'App\\Article', -5, ['title' => 'ok'], PointInTime::from('2024-01-01T10:00:00.000000+00:00'));
    }

    #[Test]
    #[Group('adversarial')]
    public function refuses_a_blank_stream_or_type(): void
    {
        // a whitespace-only stream is blank once trimmed, a key no store could match back
        $this->expectException(InvalidArgumentException::class);

        new Snapshot('   ', 'App\\Article', 1, [], PointInTime::from('2024-01-01T10:00:00.000000+00:00'));
    }

    #[Test]
    #[Group('adversarial')]
    public function refuses_a_whitespace_only_aggregate_type(): void
    {
        // the type is trimmed on its OWN arm of the guard: a valid stream must not let a blank type
        // slip through the || short-circuit, or the sweep and diagnostics lose the aggregate's name
        $this->expectException(InvalidArgumentException::class);

        new Snapshot('article-1', '   ', 1, [], PointInTime::from('2024-01-01T10:00:00.000000+00:00'));
    }

    #[Test]
    #[Group('adversarial')]
    public function refuses_a_list_state(): void
    {
        // a list would restore garbage keys; the state is the associative bag toSnapshot() returned
        $this->expectException(InvalidArgumentException::class);

        new Snapshot('article-1', 'App\\Article', 1, [1, 2], PointInTime::from('2024-01-01T10:00:00.000000+00:00')); // @phpstan-ignore argument.type (hostile on purpose: the guard under test)
    }
}
