<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests;

use Doctrine\DBAL\Exception as DbalException;
use Generator;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use stdClass;
use Storm\Aggregate\GenericAggregateIdV7;
use Storm\AggregateRepository\DefaultAggregateRepository;
use Storm\AggregateRepository\Exception\AggregateTypeMismatch;
use Storm\AggregateRepository\Exception\CorruptStreamHistory;
use Storm\AggregateRepository\Tests\Fixture\Article;
use Storm\AggregateRepository\Tests\Fixture\ArticleDrafted;
use Storm\AggregateRepository\Tests\Fixture\ArticleId;
use Storm\AggregateRepository\Tests\Fixture\ArticlePublished;
use Storm\Chronicler\Exception\InvalidPosition;
use Storm\Chronicler\Exception\NotADomainEvent;
use Storm\Chronicler\Exception\StaleVersion;
use Storm\Chronicler\Record\EventRecord;
use Storm\Chronicler\Record\SequencePosition;
use Storm\Chronicler\Store\DecisionAppend;
use Storm\Chronicler\Store\StreamReader;
use Storm\Clock\Exception\ClockException;
use Storm\Clock\PointInTime;
use Storm\Contracts\Chronicler\StorageFailure;
use Storm\Message\Header;
use Storm\Message\Message;
use Storm\Message\MessageEnricher;
use Storm\Serializer\Exception\SerializationException;
use Storm\Stream\Stream;
use Throwable;

final class DefaultAggregateRepositoryTest extends TestCase
{
    #[Test]
    public function retrieve_wraps_a_non_event_stream_row_as_a_storage_failure(): void
    {
        // A stream row whose envelope is not a domain event is a corrupt read. retrieve() translates it,
        // at the port boundary, into the contracted StorageFailure, cause preserved, rather than letting
        // the Chronicler-internal NotADomainEvent leak through the Aggregate port.
        $id = ArticleId::generate();

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveAll')->willReturn(self::stream(new EventRecord(
            new Message(new stdClass), // a non-event in an event stream
            SequencePosition::fromInt(1),
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        )));

        $repository = new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $eventStore,
            $this->createStub(DecisionAppend::class),
            $this->createStub(MessageEnricher::class),
        );

        try {
            $repository->retrieve($id);
            $this->fail('Expected the non-event row to surface as a StorageFailure.');
        } catch (StorageFailure $failure) {
            // catchable by the contracted type, and the original cause is preserved for diagnosis
            $this->assertInstanceOf(NotADomainEvent::class, $failure->getPrevious());
        }
    }

    #[Test]
    public function retrieve_replays_the_stream_and_carries_the_last_header_version(): void
    {
        // the central read path: records replay in order through reconstitute(), and the aggregate
        // version is the one stamped on the LAST record's header; that version is the OCC base the
        // next store() appends against, so a version that lags the stream is a corruption, not a detail.
        $id = ArticleId::generate();

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveAll')->willReturn(self::stream(
            new EventRecord(
                new Message(new ArticleDrafted($id->toString(), 'Hello'), [Header::AggregateVersion->key() => 1]),
                SequencePosition::fromInt(1),
                PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
            ),
            new EventRecord(
                new Message(new ArticlePublished($id->toString()), [Header::AggregateVersion->key() => 2]),
                SequencePosition::fromInt(2),
                PointInTime::from('2024-01-01T10:00:01.000000+00:00'),
            ),
        ));

        $repository = new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $eventStore,
            $this->createStub(DecisionAppend::class),
            $this->createStub(MessageEnricher::class),
        );

        $article = $repository->retrieve($id);

        $this->assertInstanceOf(Article::class, $article);
        $this->assertSame('Hello', $article->title());
        $this->assertTrue($article->published);
        $this->assertSame(2, $article->version());
    }

    #[Test]
    public function retrieve_of_an_unknown_aggregate_is_null(): void
    {
        // an unknown aggregate is an empty stream, not an error: reconstitute() yields null at version 0,
        // which is what tells a create-command it is free to append at expected version 0.
        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveAll')->willReturn(self::stream());

        $repository = new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $eventStore,
            $this->createStub(DecisionAppend::class),
            $this->createStub(MessageEnricher::class),
        );

        $this->assertNull($repository->retrieve(ArticleId::generate()));
    }

    #[Test]
    public function store_of_an_aggregate_with_nothing_released_appends_nothing(): void
    {
        // storing an aggregate no command touched must not reach the event store at all: an empty append
        // would open a transaction, take the OCC round-trip, and write a stream row for zero events. The
        // port below guards this too, but the repository is where the caller's no-op is decided.
        $article = Article::draft(ArticleId::generate(), 'Hello');
        $article->releaseEvents(); // the draft is already persisted, nothing left to release

        $eventStore = $this->createMock(DecisionAppend::class);
        $eventStore->expects($this->never())->method('appendTo');

        new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $this->createStub(StreamReader::class),
            $eventStore,
            $this->createStub(MessageEnricher::class),
        )->store($article);
    }

    #[Test]
    public function store_stamps_each_released_event_with_a_contiguous_version_and_the_identity_headers(): void
    {
        // the write-side of the OCC contract: store() appends at the version the stream held BEFORE
        // this release, so `$expected` is version minus released count, and stamps every released event with its
        // own contiguous aggregate version and the three identity headers the read path checks back.
        // A fresh draft-then-publish releases two events over a zero base, so the versions run 1, 2.
        $id = ArticleId::generate();
        $article = Article::draft($id, 'Hello');
        $article->publish();

        // a capturing port: the Stream and the expected version ARE the persisted append, so asserting
        // them pins the header/OCC arithmetic that a real store would otherwise only prove in integration
        $append = new class() implements DecisionAppend
        {
            public ?Stream $stream = null;

            public ?int $expectedVersion = null;

            public function appendTo(Stream $stream, int $expectedVersion): void
            {
                $this->stream = $stream;
                $this->expectedVersion = $expectedVersion;
            }
        };

        new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $this->createStub(StreamReader::class),
            $append,
            self::identityEnricher(),
        )->store($article);

        $this->assertSame(0, $append->expectedVersion, 'expected = version(2) - released(2)');
        $this->assertInstanceOf(Stream::class, $append->stream);
        $this->assertSame(2, $append->stream->count(), 'both released events reach the stream');

        $messages = iterator_to_array($append->stream->messages());
        $this->assertSame($id->toString(), $messages[0]->header(Header::AggregateId));
        $this->assertSame(ArticleId::class, $messages[0]->header(Header::AggregateIdType));
        $this->assertSame(Article::class, $messages[0]->header(Header::AggregateType));
        $this->assertSame(1, $messages[0]->aggregateVersion(), 'first released event is version expected + 0 + 1');
        $this->assertSame(2, $messages[1]->aggregateVersion(), 'second is expected + 1 + 1, contiguous');
    }

    #[Test]
    #[Group('adversarial')]
    public function store_translates_a_raw_write_failure_into_the_contracted_storage_failure(): void
    {
        // the write-side mirror of retrieve()'s translation: by the time control returns here the
        // retry decorator below has given up, so the port wraps the raw driver failure with cause
        // preserved; an untyped surface would force callers to inspect concrete driver classes
        $article = Article::draft(ArticleId::generate(), 'Hello');

        $append = new class() implements DecisionAppend
        {
            public function appendTo(Stream $stream, int $expectedVersion): void
            {
                throw new class('connection lost') extends RuntimeException implements DbalException {};
            }
        };

        try {
            new DefaultAggregateRepository(
                Article::class,
                ArticleId::class,
                'article',
                $this->createStub(StreamReader::class),
                $append,
                self::identityEnricher(),
            )->store($article);
            $this->fail('expected the raw write failure to cross the port as the contracted StorageFailure');
        } catch (StorageFailure $e) {
            $this->assertInstanceOf(DbalException::class, $e->getPrevious(), 'the raw cause is preserved for diagnostics');
        }
    }

    #[Test]
    #[Group('adversarial')]
    public function store_translates_a_serialization_failure_into_the_contracted_storage_failure(): void
    {
        // the second arm of the same catch, and the one no driver produces: an event the codec
        // cannot write, a personal key missing its subject or a payload that will not encode. It
        // reaches this port as the Serializer's own contract, and a caller that only knows
        // StorageFailure would let it escape untyped, out of a store() whose clause names neither
        $article = Article::draft(ArticleId::generate(), 'Hello');
        $unwritable = SerializationException::missingPersonalSubject('ArticlePublished', 'author_id');

        $append = new readonly class($unwritable) implements DecisionAppend
        {
            public function __construct(private SerializationException $failure) {}

            public function appendTo(Stream $stream, int $expectedVersion): void
            {
                throw $this->failure;
            }
        };

        try {
            new DefaultAggregateRepository(
                Article::class,
                ArticleId::class,
                'article',
                $this->createStub(StreamReader::class),
                $append,
                self::identityEnricher(),
            )->store($article);
            $this->fail('expected the serialization failure to cross the port as the contracted StorageFailure');
        } catch (StorageFailure $e) {
            $this->assertSame($unwritable, $e->getPrevious(), 'the codec failure is preserved for diagnostics');
        }
    }

    #[Test]
    public function store_lets_a_concurrency_conflict_pass_untranslated(): void
    {
        // the concurrency contracts are not infra failures: StaleVersion must reach the caller AS
        // ITSELF, since the recover middleware retries on the RetryableConcurrencyConflict marker,
        // which a StorageFailure wrap would strip
        $article = Article::draft(ArticleId::generate(), 'Hello');

        $append = new class() implements DecisionAppend
        {
            public function appendTo(Stream $stream, int $expectedVersion): void
            {
                throw StaleVersion::forStream('article-1', $expectedVersion, 7);
            }
        };

        $this->expectException(StaleVersion::class);

        new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $this->createStub(StreamReader::class),
            $append,
            self::identityEnricher(),
        )->store($article);
    }

    #[Test]
    public function store_appends_a_reconstituted_aggregates_new_events_at_the_current_stream_version(): void
    {
        // the OCC continuation, the realistic multi-store path a create-then-update takes: an aggregate
        // reconstituted at version 1 that records one more event must append at expected version 1,
        // the base the stream already holds, NOT 0, and stamp the new event as version 2.
        $id = ArticleId::generate();

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveAll')->willReturn(self::stream(new EventRecord(
            new Message(new ArticleDrafted($id->toString(), 'Hello'), [Header::AggregateVersion->key() => 1]),
            SequencePosition::fromInt(1),
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        )));

        $append = new class() implements DecisionAppend
        {
            public ?Stream $stream = null;

            public ?int $expectedVersion = null;

            public function appendTo(Stream $stream, int $expectedVersion): void
            {
                $this->stream = $stream;
                $this->expectedVersion = $expectedVersion;
            }
        };

        $repository = new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $eventStore,
            $append,
            self::identityEnricher(),
        );

        $article = $repository->retrieve($id);
        $this->assertInstanceOf(Article::class, $article);

        $article->publish();
        $repository->store($article);

        $this->assertSame(1, $append->expectedVersion, 'expected = version(2) - released(1)');
        $this->assertInstanceOf(Stream::class, $append->stream);
        $this->assertSame(1, $append->stream->count());

        $messages = iterator_to_array($append->stream->messages());
        $this->assertSame(2, $messages[0]->aggregateVersion(), 'the new event stamps version expected(1) + 0 + 1');
    }

    private static function identityEnricher(): MessageEnricher
    {
        return new class() implements MessageEnricher
        {
            public function enrich(Message $message): Message
            {
                return $message;
            }
        };
    }

    #[Test]
    public function retrieve_wraps_a_dbal_read_failure_as_a_storage_failure(): void
    {
        // the StreamReader port does not contract DBAL failures; they surface raw. retrieve() catches
        // and translates them, so the DBAL arm of the catch union is genuinely live.
        $dbal = new class('read failed') extends RuntimeException implements DbalException {};

        $failure = $this->retrieveFailureOf($dbal);

        $this->assertSame($dbal, $failure->getPrevious());
    }

    #[Test]
    public function retrieve_wraps_a_corrupt_stored_position_as_a_storage_failure(): void
    {
        // a stored position that is not a positive integer is a corrupt row: the Chronicler-internal
        // InvalidPosition must not leak through the Aggregate port.
        $corruptPosition = InvalidPosition::notAPositiveInteger('0');

        $failure = $this->retrieveFailureOf($corruptPosition);

        $this->assertSame($corruptPosition, $failure->getPrevious());
    }

    #[Test]
    public function retrieve_wraps_a_stored_payload_that_cannot_be_deserialized_as_a_storage_failure(): void
    {
        // a stored payload whose event type no longer maps to a loadable class is a corrupt row,
        // contracted as StorageFailure, not as the Serializer-internal exception.
        $undeserializable = SerializationException::cannotDeserialize('article.published');

        $failure = $this->retrieveFailureOf($undeserializable);

        $this->assertSame($undeserializable, $failure->getPrevious());
    }

    #[Test]
    public function retrieve_wraps_an_unparseable_stored_point_in_time_as_a_storage_failure(): void
    {
        // an occurred-at that no longer parses is a corrupt row too; the Clock-internal exception is
        // translated at the port boundary like every other read corruption.
        $unparseableClock = new ClockException('the stored point in time is not parseable');

        $failure = $this->retrieveFailureOf($unparseableClock);

        $this->assertSame($unparseableClock, $failure->getPrevious());
    }

    /**
     * Drives retrieve() against a reader whose stream fails, and returns the contracted failure.
     *
     * The reader hydrates lazily: a corrupt row raises while the stream is being consumed, not when
     * `retrieveAll()` is called. Failing from inside the generator reproduces that, and so also pins
     * that the try block spans the consumption of the stream, not merely the call that opens it.
     */
    private function retrieveFailureOf(Throwable $readFailure): StorageFailure
    {
        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveAll')->willReturn(self::failingStream($readFailure));

        $repository = new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $eventStore,
            $this->createStub(DecisionAppend::class),
            $this->createStub(MessageEnricher::class),
        );

        try {
            $repository->retrieve(ArticleId::generate());
        } catch (StorageFailure $failure) {
            return $failure;
        }

        $this->fail(sprintf('Expected the %s read failure to surface as a StorageFailure.', $readFailure::class));
    }

    private static function stream(EventRecord ...$records): Generator
    {
        yield from $records;
    }

    /**
     * @return Generator<int, EventRecord>
     */
    private static function failingStream(Throwable $readFailure): Generator
    {
        yield from [];

        throw $readFailure;
    }

    #[Test]
    #[Group('adversarial')]
    public function a_gap_in_the_stream_is_corruption_not_state(): void
    {
        // continuity is validated where the record coordinates are still visible: versions run 1, 2,
        // and so on; a record jumping ahead means events are missing, and a decision on the partial
        // state could pass the store's CAS and become a durable fact
        $id = ArticleId::generate();

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveAll')->willReturn(self::stream(
            new EventRecord(
                new Message(new ArticleDrafted($id->toString(), 'Hello'), [Header::AggregateVersion->key() => 1]),
                SequencePosition::fromInt(1),
                PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
            ),
            new EventRecord(
                new Message(new ArticlePublished($id->toString()), [Header::AggregateVersion->key() => 3]), // gap: expected 2
                SequencePosition::fromInt(3),
                PointInTime::from('2024-01-01T10:00:01.000000+00:00'),
            ),
        ));

        $this->expectException(CorruptStreamHistory::class);
        // the reported OCC base is $version + 1, here 1 + 1: a wrong arm would misdirect the operator
        $this->expectExceptionMessageIsOrContains('expected version 2, observed 3');

        self::repository($eventStore)->retrieve($id);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_record_without_a_version_header_is_corruption(): void
    {
        // an unheadered record cannot participate in the OCC token, so its state cannot be trusted either
        $id = ArticleId::generate();

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveAll')->willReturn(self::stream(new EventRecord(
            new Message(new ArticleDrafted($id->toString(), 'Hello')), // no AggregateVersion header
            SequencePosition::fromInt(1),
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        )));

        $this->expectException(CorruptStreamHistory::class);
        // the first record fails at seed $version 0, so version 0 + 1 was expected
        $this->expectExceptionMessageIsOrContains('version 1 was expected');

        self::repository($eventStore)->retrieve($id);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_foreign_events_state_is_never_laundered_into_the_requested_aggregate(): void
    {
        // the read-side mirror of store()'s own id check: an imported or foreign row carrying another
        // aggregate's id would otherwise apply silently, its state adopted under THIS aggregate's identity
        $id = ArticleId::generate();
        $other = ArticleId::generate();

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveAll')->willReturn(self::stream(new EventRecord(
            new Message(new ArticleDrafted($other->toString(), 'stolen'), [Header::AggregateVersion->key() => 1]),
            SequencePosition::fromInt(1),
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        )));

        $this->expectException(CorruptStreamHistory::class);

        self::repository($eventStore)->retrieve($id);
    }

    #[Test]
    #[Group('adversarial')]
    public function retrieve_refuses_a_foreign_identity_class(): void
    {
        // the mirror of store()'s check: PHPStan refuses this call statically because the contract
        // carries TId, so the ignore below is the point of the test; the runtime guard holds
        // for every caller the static layer cannot see, concrete types and dynamic wiring
        $eventStore = $this->createStub(StreamReader::class);

        $this->expectException(AggregateTypeMismatch::class);

        self::repository($eventStore)->retrieve(GenericAggregateIdV7::fromString(ArticleId::generate()->toString())); // @phpstan-ignore argument.type (deliberate foreign id: proves the runtime guard behind the static one)
    }

    /**
     * @return DefaultAggregateRepository<ArticleId, Article>
     */
    private static function repository(StreamReader $eventStore): DefaultAggregateRepository
    {
        return new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $eventStore,
            new class() implements DecisionAppend
            {
                public function appendTo(Stream $stream, int $expectedVersion): void {}
            },
            new class() implements MessageEnricher
            {
                public function enrich(Message $message): Message
                {
                    return $message;
                }
            },
        );
    }

    #[Test]
    public function a_store_that_threw_leaves_the_instance_spent_and_a_retry_is_a_no_op(): void
    {
        // the DOCUMENTED single-shot semantics, not an accident: releaseEvents() drained the buffer
        // before the append, so after a failed store the instance has nothing left to persist while
        // its version stays advanced; recovery is reload-and-re-decide, never a second store()
        $id = ArticleId::generate();
        $article = Article::draft($id, 'Hello');

        $append = new class() implements DecisionAppend
        {
            public int $calls = 0;

            public function appendTo(Stream $stream, int $expectedVersion): void
            {
                $this->calls++;

                throw new RuntimeException('append failed');
            }
        };

        $repository = new DefaultAggregateRepository(
            Article::class,
            ArticleId::class,
            'article',
            $this->createStub(StreamReader::class),
            $append,
            new class() implements MessageEnricher
            {
                public function enrich(Message $message): Message
                {
                    return $message;
                }
            },
        );

        try {
            $repository->store($article);
            $this->fail('expected the arranged append failure');
        } catch (RuntimeException) {
        }

        $repository->store($article); // spent: nothing to release, silent no-op by contract

        $this->assertSame(1, $append->calls, 'the retry appended nothing — the instance was spent');
        $this->assertSame(1, $article->version(), 'the version stays advanced past the store the stream never saw');
    }
}
