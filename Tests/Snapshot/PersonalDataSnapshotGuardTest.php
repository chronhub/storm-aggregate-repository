<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Snapshot;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use Storm\AggregateRepository\Snapshot\PersonalDataSnapshotGuard;
use Storm\AggregateRepository\Tests\Fixture\ArticleDrafted;
use Storm\AggregateRepository\Tests\Fixture\ArticlePublished;
use Storm\Chronicler\Evolution\IdentityEventTypeMapper;
use Storm\Contracts\Chronicler\EventTypeMapper;

/**
 * The guard's duties at its OWN boundary, the Connection mocked: an empty map probes nothing, the
 * probe binds the stream AND its derived category so the planner prunes to one partition of the
 * LIST-partitioned event_store, and the row answer maps to the offending alias or null. The
 * integration SnapshotSweepCommandTest proves the refusal against a real store; a mock cannot.
 */
final class PersonalDataSnapshotGuardTest extends TestCase
{
    public const string MARKED = ArticleDrafted::class;

    public const string OTHER = ArticlePublished::class;

    #[Test]
    public function an_empty_map_answers_clean_without_probing_the_store(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects($this->never())->method('fetchOne');

        $guard = new PersonalDataSnapshotGuard($connection, new IdentityEventTypeMapper, []);

        $this->assertNull($guard->refusal('account-42'));
    }

    #[Test]
    public function the_probe_binds_the_category_derived_from_the_stream_beside_the_stream_itself(): void
    {
        // the category param is the partition-pruning predicate: dropping it re-opens a probe over
        // EVERY partition of event_store (behavior-identical, cost-catastrophic), so it is pinned
        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())->method('fetchOne')
            ->with($this->anything(), [
                'category' => 'account',
                'stream' => 'account-42',
                'types' => [self::MARKED],
            ])
            ->willReturn(self::MARKED);

        $guard = new PersonalDataSnapshotGuard($connection, new IdentityEventTypeMapper, [
            self::MARKED => ['subject' => 'articleId', 'keys' => ['title'], 'fallbacks' => []],
        ]);

        $this->assertSame(self::MARKED, $guard->refusal('account-42'));
    }

    #[Test]
    #[Group('adversarial')]
    public function the_probe_asks_for_every_alias_of_every_marked_class(): void
    {
        // Two ways to lose an alias here, and both are silent: keeping one class's types instead of
        // merging them all, and keeping one alias per class instead of every spelling it was ever
        // stored under. Either way the probe asks a narrower question, finds nothing, and lets a
        // snapshot freeze personal data that crypto-shredding can no longer reach.
        //
        // Both need a fixture wide enough to show it: a single class with a single alias, which is
        // what an identity mapper hands back, cannot tell any of the three shapes apart.
        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())->method('fetchOne')
            ->with(
                $this->anything(),
                [
                    'category' => 'account',
                    'stream' => 'account-42',
                    'types' => ['article.drafted', 'article.written', self::MARKED, 'article.published', self::OTHER],
                ],
                ['types' => ArrayParameterType::STRING], // the array binding; unbound, the IN list is one scalar
            )
            ->willReturn('article.written');

        $guard = new PersonalDataSnapshotGuard($connection, $this->mapperWithFormerAliases(), [
            self::MARKED => ['subject' => 'articleId', 'keys' => ['title'], 'fallbacks' => []],
            self::OTHER => ['subject' => 'articleId', 'keys' => ['body'], 'fallbacks' => []],
        ]);

        $this->assertSame('article.written', $guard->refusal('account-42'));
    }

    /**
     * A mapper whose classes each answer with several stored types, the shape a renamed event has:
     * the current alias, a former one, and the FQCN its pre-alias rows carry.
     */
    private function mapperWithFormerAliases(): EventTypeMapper
    {
        return new class() implements EventTypeMapper
        {
            public function toType(string $class): string
            {
                return $class;
            }

            public function toClass(string $type): string
            {
                return PersonalDataSnapshotGuardTest::MARKED; // never consulted by the guard
            }

            public function versionOf(string $class): int
            {
                return 1;
            }

            public function storedTypesOf(string $class): array
            {
                return $class === PersonalDataSnapshotGuardTest::MARKED
                    ? ['article.drafted', 'article.written', $class]
                    : ['article.published', $class];
            }
        };
    }

    #[Test]
    public function a_stream_holding_no_marked_row_answers_clean(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects($this->once())->method('fetchOne')->willReturn(false);

        $guard = new PersonalDataSnapshotGuard($connection, new IdentityEventTypeMapper, [
            self::MARKED => ['subject' => 'articleId', 'keys' => ['title'], 'fallbacks' => []],
        ]);

        $this->assertNull($guard->refusal('account-42'));
    }
}
