<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Query\QueryBuilder;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Storm\Aggregate\GenericAggregateIdV7;
use Storm\AggregateRepository\Exception\AggregateTypeMismatch;
use Storm\AggregateRepository\Exception\CorruptStreamHistory;
use Storm\AggregateRepository\Exception\InspectionVersionOutOfRange;
use Storm\AggregateRepository\HistoricalAggregateInspector;
use Storm\AggregateRepository\Tests\Fixture\ArticleDrafted;
use Storm\AggregateRepository\Tests\Fixture\ArticleId;
use Storm\AggregateRepository\Tests\Fixture\SnapshotArticle;
use Storm\Chronicler\Exception\InvalidPosition;
use Storm\Chronicler\Exception\NotADomainEvent;
use Storm\Chronicler\Query\AggregateStreamUpTo;
use Storm\Chronicler\Query\QueryFilter;
use Storm\Chronicler\Record\EventRecord;
use Storm\Chronicler\Record\SequencePosition;
use Storm\Chronicler\Store\StreamReader;
use Storm\Clock\PointInTime;
use Storm\Contracts\Chronicler\StorageFailure;
use Storm\Contracts\Clock\ClockExceptionContract;
use Storm\Contracts\Serializer\SerializationExceptionContract;
use Storm\Message\Header;
use Storm\Message\Message;
use Storm\Stream\StreamCategory;
use Throwable;

/**
 * The repository boundary's read-only historical fold: exact target, SQL-bounded read, no partial
 * state from a short history.
 */
final class HistoricalAggregateInspectorTest extends TestCase
{
    #[Test]
    public function it_folds_exactly_the_requested_version_into_a_detached_state(): void
    {
        $id = ArticleId::generate();
        $filters = [];

        $state = self::inspector($this->reader($filters, self::drafted($id, 'First', 1), self::drafted($id, 'Second', 2)))
            ->stateAt($id, 2);

        self::assertSame(2, $state->version);
        self::assertSame('Second', $state->state['title']);
        self::assertCount(1, $filters);
        self::assertInstanceOf(AggregateStreamUpTo::class, $filters[0]);
        self::assertSame('article', $filters[0]->category->value);
        self::assertSame('article-'.$id->toString(), $filters[0]->stream);
        self::assertSame(2, $filters[0]->upToVersion);
    }

    #[Test]
    #[Group('adversarial')]
    public function the_read_is_bounded_in_sql_by_the_target(): void
    {
        // the bound lives in the statement, so an event appended past the target after the head was
        // observed cannot reach the fold, and PHP never discards rows the driver already buffered
        $connection = $this->createStub(Connection::class);
        $connection->method('getDatabasePlatform')->willReturn(new PostgreSQLPlatform);
        $query = new QueryBuilder($connection)->select('e.version')->from('event_store', 'e');

        new AggregateStreamUpTo(new StreamCategory('article'), 'article-1', 7)->apply($query);

        $sql = $query->getSQL();
        self::assertStringContainsString('e.version <= :upToVersion', $sql);
        self::assertStringContainsString('ORDER BY e.version ASC', $sql);
        self::assertMatchesRegularExpression('/LIMIT\s+7\b/', $sql);
        self::assertSame(7, $query->getParameter('upToVersion'));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_history_that_stops_short_of_the_target_is_corruption_not_a_partial_state(): void
    {
        $id = ArticleId::generate();
        $filters = [];

        $this->expectException(CorruptStreamHistory::class);
        $this->expectExceptionMessage('stops at version 1 while its head claims at least version 2');

        self::inspector($this->reader($filters, self::drafted($id, 'First', 1)))->stateAt($id, 2);
    }

    #[Test]
    #[Group('adversarial')]
    public function an_empty_history_under_a_claimed_target_is_corruption(): void
    {
        $id = ArticleId::generate();
        $filters = [];

        $this->expectException(CorruptStreamHistory::class);
        $this->expectExceptionMessage('stops at version 0');

        self::inspector($this->reader($filters))->stateAt($id, 1);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_gap_below_the_target_keeps_the_replay_guard(): void
    {
        $id = ArticleId::generate();
        $filters = [];

        $this->expectException(CorruptStreamHistory::class);
        $this->expectExceptionMessage('expected version 2, observed 3');

        self::inspector($this->reader($filters, self::drafted($id, 'First', 1), self::drafted($id, 'Third', 3)))->stateAt($id, 3);
    }

    #[Test]
    public function a_foreign_identity_is_refused_before_reading_a_stream_with_the_same_string(): void
    {
        $id = ArticleId::generate();
        $foreign = GenericAggregateIdV7::fromString($id->toString());
        self::assertSame($id->toString(), $foreign->toString());
        $reader = $this->createMock(StreamReader::class);
        $reader->expects($this->never())->method('retrieveByFilter');
        $this->expectException(AggregateTypeMismatch::class);

        self::inspector($reader)->stateAt($foreign, 1);
    }

    #[Test]
    public function the_lenient_fold_names_how_far_a_short_stream_reaches(): void
    {
        $id = ArticleId::generate();
        $filters = [];

        $state = self::inspector($this->reader($filters, self::drafted($id, 'First', 1)))->foldUpTo($id, 3);

        self::assertNotNull($state);
        self::assertSame(1, $state->version);
        self::assertNull(self::inspector($this->reader($filters))->foldUpTo($id, 3));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_target_outside_the_foldable_range_is_refused_before_any_read(): void
    {
        $reader = $this->createMock(StreamReader::class);
        $reader->expects($this->never())->method('retrieveByFilter');
        $inspector = self::inspector($reader);
        $id = ArticleId::generate();

        foreach ([0, -1, HistoricalAggregateInspector::MAX_VERSIONS + 1] as $version) {
            try {
                $inspector->stateAt($id, $version);
                self::fail(sprintf('version %d must be refused', $version));
            } catch (InspectionVersionOutOfRange) {
                // refused before the read, as the reader's expectation asserts
            }
        }
    }

    #[Test]
    #[Group('adversarial')]
    public function a_driver_failure_crosses_as_the_contracted_storage_failure(): void
    {
        $reader = $this->createStub(StreamReader::class);
        $reader->method('retrieveByFilter')->willThrowException(new class('the store is unreachable') extends RuntimeException implements DbalException {});

        $this->expectException(StorageFailure::class);

        self::inspector($reader)->stateAt(ArticleId::generate(), 1);
    }

    #[Test]
    #[DataProvider('readFailures')]
    public function each_read_failure_crosses_as_the_contracted_storage_failure(Throwable $failure): void
    {
        $reader = $this->createStub(StreamReader::class);
        $reader->method('retrieveByFilter')->willThrowException($failure);

        try {
            self::inspector($reader)->stateAt(ArticleId::generate(), 1);
            self::fail('a read failure must cross as the contracted storage failure');
        } catch (StorageFailure $e) {
            self::assertSame($failure, $e->getPrevious());
        }
    }

    /**
     * @return iterable<string, array{Throwable}>
     */
    public static function readFailures(): iterable
    {
        yield 'an invalid position' => [InvalidPosition::notAPositiveInteger('zero')];
        yield 'a serialization failure' => [new class('an undecodable payload') extends RuntimeException implements SerializationExceptionContract {}];
        yield 'a clock failure' => [new class('an unreadable recorded instant') extends RuntimeException implements ClockExceptionContract {}];
        yield 'a record that is no domain event' => [NotADomainEvent::got(new stdClass)];
    }

    #[Test]
    public function the_ceiling_itself_is_a_foldable_target(): void
    {
        // the last version a caller may ask for reaches the read, and a history that stops short of it
        // is refused as corruption, never as a version out of range
        $filters = [];

        $this->expectException(CorruptStreamHistory::class);

        self::inspector($this->reader($filters))->stateAt(ArticleId::generate(), HistoricalAggregateInspector::MAX_VERSIONS);
    }

    private static function inspector(StreamReader $reader): HistoricalAggregateInspector
    {
        return new HistoricalAggregateInspector(SnapshotArticle::class, ArticleId::class, new StreamCategory('article'), $reader);
    }

    /**
     * @param  list<QueryFilter>  $filters  receives every filter the inspector reads through
     */
    private function reader(array &$filters, EventRecord ...$records): StreamReader
    {
        $reader = $this->createStub(StreamReader::class);
        $reader->method('retrieveByFilter')->willReturnCallback(static function (QueryFilter $filter) use (&$filters, $records): Generator {
            $filters[] = $filter;

            yield from $records;
        });

        return $reader;
    }

    private static function drafted(ArticleId $id, string $title, int $version): EventRecord
    {
        return new EventRecord(
            new Message(new ArticleDrafted($id->toString(), $title), [Header::AggregateVersion->key() => $version]),
            SequencePosition::fromInt($version),
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        );
    }
}
