<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests;

use Generator;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Storm\AggregateRepository\AggregateRepositoryManager;
use Storm\AggregateRepository\DefaultAggregateRepository;
use Storm\AggregateRepository\Exception\UnknownAggregate;
use Storm\AggregateRepository\Snapshot\Snapshot;
use Storm\AggregateRepository\Snapshot\SnapshotStore;
use Storm\AggregateRepository\SnapshotRepository;
use Storm\AggregateRepository\Tests\Fixture\Article;
use Storm\AggregateRepository\Tests\Fixture\ArticleId;
use Storm\AggregateRepository\Tests\Fixture\SnapshotArticle;
use Storm\Chronicler\Directory\StreamHeadStore;
use Storm\Chronicler\Query\QueryFilter;
use Storm\Chronicler\Store\DecisionAppend;
use Storm\Chronicler\Store\Direction;
use Storm\Chronicler\Store\StreamReader;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Aggregate\AggregateRepository;
use Storm\Contracts\Chronicler\Position;
use Storm\Message\Message;
use Storm\Message\MessageEnricher;
use Storm\Stream\Stream;
use Storm\Stream\StreamName;

final class AggregateRepositoryManagerTest extends TestCase
{
    #[Test]
    public function builds_a_repository_for_a_configured_aggregate(): void
    {
        $manager = $this->managerFor([Article::class => ['id' => ArticleId::class, 'category' => 'article']]);

        $this->assertTrue(is_subclass_of($manager->for(Article::class), AggregateRepository::class));
    }

    #[Test]
    public function caches_one_repository_per_aggregate(): void
    {
        $manager = $this->managerFor([Article::class => ['id' => ArticleId::class, 'category' => 'article']]);

        $this->assertSame($manager->for(Article::class), $manager->for(Article::class));
    }

    #[Test]
    public function throws_for_an_unconfigured_aggregate(): void
    {
        $this->expectException(UnknownAggregate::class);

        $this->managerFor([])->for(Article::class);
    }

    #[Test]
    public function wraps_a_snapshotable_aggregate_in_a_snapshot_repository(): void
    {
        $manager = $this->managerFor([SnapshotArticle::class => ['id' => ArticleId::class, 'category' => 'article']]);

        $this->assertInstanceOf(SnapshotRepository::class, $manager->for(SnapshotArticle::class));
    }

    #[Test]
    public function does_not_wrap_a_plain_aggregate(): void
    {
        $manager = $this->managerFor([Article::class => ['id' => ArticleId::class, 'category' => 'article']]);

        $this->assertInstanceOf(DefaultAggregateRepository::class, $manager->for(Article::class));
    }

    /**
     * @param  array<class-string, array{id: class-string<AggregateIdentity>, category: string}>  $aggregates
     */
    private function managerFor(array $aggregates): AggregateRepositoryManager
    {
        $streamReader = new class() implements StreamReader
        {
            public function retrieveAll(StreamName $streamName, Direction $direction = Direction::Forward): Generator
            {
                yield from [];
            }

            public function retrieveByFilter(QueryFilter $filter): Generator
            {
                yield from [];
            }

            public function safeHeadPosition(): ?Position
            {
                return null;
            }
        };

        $decisionAppend = new class() implements DecisionAppend
        {
            public function appendTo(Stream $stream, int $expectedVersion): void {}
        };

        $enricher = new class() implements MessageEnricher
        {
            public function enrich(Message $message): Message
            {
                return $message;
            }
        };

        $snapshots = new class() implements SnapshotStore
        {
            public function load(string $stream): ?Snapshot
            {
                return null;
            }

            public function save(Snapshot $snapshot): void {}

            public function delete(string $stream): bool
            {
                return false;
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

        $heads = new class() implements StreamHeadStore
        {
            public function advance(string $stream, int $expectedVersion, int $newVersion): bool
            {
                return true;
            }

            public function bump(string $stream, int $count): int
            {
                return $count;
            }

            public function lastVersion(string $stream): int
            {
                return PHP_INT_MAX; // always coherent
            }

            public function lockForErase(string $stream): void {}

            public function delete(string $stream): bool
            {
                return false;
            }
        };

        return new AggregateRepositoryManager($aggregates, $streamReader, $decisionAppend, $enricher, $snapshots, $heads);
    }
}
