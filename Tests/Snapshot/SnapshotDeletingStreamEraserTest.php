<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Snapshot;

use ArrayObject;
use Closure;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Storm\AggregateRepository\Exception\SnapshotStreamBusy;
use Storm\AggregateRepository\Snapshot\Snapshot;
use Storm\AggregateRepository\Snapshot\SnapshotDeletingStreamEraser;
use Storm\AggregateRepository\Snapshot\SnapshotStore;
use Storm\AggregateRepository\Snapshot\SnapshotStreamFence;
use Storm\Chronicler\Erasure\StreamEraser;
use Storm\Stream\StreamName;

final class SnapshotDeletingStreamEraserTest extends TestCase
{
    #[Test]
    public function deletes_the_snapshot_first_then_erases_and_returns_the_erase_count(): void
    {
        // snapshot FIRST is the load-bearing order: a crash between the two must leave a live stream
        // without its cache, harmless and re-swept, never an erased stream with a surviving snapshot
        // that would resurrect the orphan or pollute a re-created id
        $log = new ArrayObject;

        $snapshots = $this->snapshotsRecording($log);
        $eraser = new readonly class($log) implements StreamEraser
        {
            /** @param ArrayObject<int, string> $log */
            public function __construct(private ArrayObject $log) {}

            public function erase(StreamName $streamName): int
            {
                $this->log->append('erase:'.$streamName->toString());

                return 7;
            }
        };

        $erased = new SnapshotDeletingStreamEraser($eraser, $snapshots, $this->fence(granted: true, log: $log))
            ->erase(new StreamName('article')->withQualifier('a1'));

        $this->assertSame(7, $erased);
        $this->assertSame(['fence:article-a1', 'delete:article-a1', 'erase:article-a1'], $log->getArrayCopy());
    }

    #[Test]
    #[Group('adversarial')]
    public function an_erase_failure_propagates_with_the_snapshot_delete_left_to_the_fence_to_undo(): void
    {
        // the failure is the caller's to see, never swallowed: the fence's transaction is what puts
        // the snapshot row back, and it can only do that if the throwable crosses it
        $log = new ArrayObject;
        $snapshots = $this->snapshotsRecording($log);

        $eraser = new class() implements StreamEraser
        {
            public function erase(StreamName $streamName): int
            {
                throw new RuntimeException('erase failed');
            }
        };

        try {
            new SnapshotDeletingStreamEraser($eraser, $snapshots, $this->fence(granted: true, log: $log))
                ->erase(new StreamName('article')->withQualifier('a1'));
            $this->fail('expected the arranged erase failure');
        } catch (RuntimeException $e) {
            $this->assertSame('erase failed', $e->getMessage());
        }

        $this->assertSame(['fence:article-a1', 'delete:article-a1'], $log->getArrayCopy());
    }

    #[Test]
    #[Group('adversarial')]
    public function a_stream_held_by_a_sweep_refuses_the_erase_before_anything_is_deleted(): void
    {
        // ORDER, not net effect: the refusal must land before the snapshot DELETE is issued, so a
        // caller that retries is retrying against a store nothing has touched
        $log = new ArrayObject;
        $snapshots = $this->snapshotsRecording($log);

        $eraser = new class() implements StreamEraser
        {
            public function erase(StreamName $streamName): int
            {
                throw new RuntimeException('the inner erase must never be reached');
            }
        };

        try {
            new SnapshotDeletingStreamEraser($eraser, $snapshots, $this->fence(granted: false, log: $log))
                ->erase(new StreamName('article')->withQualifier('a1'));
            $this->fail('expected the busy refusal');
        } catch (SnapshotStreamBusy $e) {
            $this->assertSame('article-a1', $e->stream);
            $this->assertStringContainsString('article-a1', $e->getMessage());
            $this->assertStringContainsString('retry', $e->getMessage());
        }

        $this->assertSame(['fence:article-a1'], $log->getArrayCopy());
    }

    /**
     * @param  ArrayObject<int, string>  $log
     */
    private function fence(bool $granted, ArrayObject $log): SnapshotStreamFence
    {
        return new readonly class($granted, $log) implements SnapshotStreamFence
        {
            /** @param ArrayObject<int, string> $log */
            public function __construct(private bool $granted, private ArrayObject $log) {}

            public function tryBounded(string $stream, Closure $work): bool
            {
                return $this->tryWithin($stream, $work);
            }

            public function tryWithin(string $stream, Closure $work): bool
            {
                $this->log->append('fence:'.$stream);

                if (! $this->granted) {
                    return false;
                }

                $work();

                return true;
            }
        };
    }

    /**
     * @param  ArrayObject<int, string>  $log
     */
    private function snapshotsRecording(ArrayObject $log): SnapshotStore
    {
        return new readonly class($log) implements SnapshotStore
        {
            /** @param ArrayObject<int, string> $log */
            public function __construct(private ArrayObject $log) {}

            public function load(string $stream): ?Snapshot
            {
                return null;
            }

            public function save(Snapshot $snapshot): void {}

            public function delete(string $stream): bool
            {
                $this->log->append('delete:'.$stream);

                return true;
            }

            public function staleStreams(string $category, int $threshold, ?int $maxAge, ?int $minInterval, int $batch, ?string $after = null): array
            {
                return [];
            }

            public function countOrphans(): int
            {
                return 0;
            }

            public function pruneOrphans(int $batch): int
            {
                return 0;
            }
        };
    }
}
