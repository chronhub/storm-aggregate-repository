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
use Storm\AggregateRepository\Exception\AggregateTypeMismatch;
use Storm\AggregateRepository\Exception\CorruptStreamHistory;
use Storm\AggregateRepository\Snapshot\Snapshot;
use Storm\AggregateRepository\Snapshot\SnapshotStore;
use Storm\AggregateRepository\SnapshotRepository;
use Storm\AggregateRepository\Tests\Fixture\ArticleId;
use Storm\AggregateRepository\Tests\Fixture\ArticlePublished;
use Storm\AggregateRepository\Tests\Fixture\PickySnapshotArticle;
use Storm\AggregateRepository\Tests\Fixture\SnapshotArticle;
use Storm\Chronicler\Directory\StreamHeadStore;
use Storm\Chronicler\Exception\InvalidPosition;
use Storm\Chronicler\Exception\NotADomainEvent;
use Storm\Chronicler\Record\EventRecord;
use Storm\Chronicler\Record\SequencePosition;
use Storm\Chronicler\Store\StreamReader;
use Storm\Clock\Exception\ClockException;
use Storm\Clock\PointInTime;
use Storm\Contracts\Aggregate\AggregateRepository;
use Storm\Contracts\Chronicler\StorageFailure;
use Storm\Message\Header;
use Storm\Message\Message;
use Storm\Serializer\Exception\SerializationException;
use Throwable;

final class SnapshotRepositoryTest extends TestCase
{
    #[Test]
    #[Group('adversarial')]
    public function retrieve_refuses_a_foreign_identity_class_before_touching_the_snapshot(): void
    {
        // the decorator mirrors the inner repository's guard: every check on the snapshot-hit path
        // is STRING-based, so without this refusal a foreign id class whose string collides would
        // be served a hydrated aggregate instead of AggregateTypeMismatch
        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->never())->method('retrieve');

        $snapshots = $this->createMock(SnapshotStore::class);
        $snapshots->expects($this->never())->method('load');

        $repository = new SnapshotRepository(
            $inner,
            $snapshots,
            $this->createStub(StreamReader::class),
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        $this->expectException(AggregateTypeMismatch::class);

        $repository->retrieve(GenericAggregateIdV7::generate());
    }

    #[Test]
    public function store_delegate_to_inner_repository(): void
    {
        $id = ArticleId::generate();
        $article = SnapshotArticle::draft($id, 'Test Article');

        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->once())->method('store')->with($article);

        $snapshots = $this->createMock(SnapshotStore::class);
        $snapshots->expects($this->never())->method('save');

        $repository = new SnapshotRepository(
            $inner,
            $snapshots,
            $this->createStub(StreamReader::class),
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        $repository->store($article);
    }

    #[Test]
    public function a_snapshot_of_a_different_aggregate_type_is_a_cache_miss(): void
    {
        // a category collision could surface a snapshot written by another aggregate; restoring it into
        // this repository's class would corrupt state, so a type mismatch falls through to a type-safe
        // full reconstitution through the inner repository, never reaching fromSnapshot.
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(
            new Snapshot('article-x', 'App\\OtherAggregate', 5, ['title' => 'x'], PointInTime::from('2024-01-01T10:00:00.000000+00:00')),
        );

        // returning the fallback, not a snapshot-restored instance, is itself the proof the mismatch
        // short-circuited to the inner repository before fromSnapshot.
        $fallback = SnapshotArticle::draft($id, 'from events');
        $inner = $this->createStub(AggregateRepository::class);
        $inner->method('retrieve')->willReturn($fallback);

        $repository = new SnapshotRepository(
            $inner,
            $snapshots,
            $this->createStub(StreamReader::class),
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        $this->assertSame($fallback, $repository->retrieve($id));
    }

    #[Test]
    public function a_missing_snapshot_falls_through_to_full_reconstitution(): void
    {
        // the cold-cache path: no snapshot yet, so the inner repository full-replays; the event-store
        // tail read belongs to the snapshot path only and must not be touched.
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(null);

        $fallback = SnapshotArticle::draft($id, 'from events');
        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->once())->method('retrieve')->with($id)->willReturn($fallback);

        $eventStore = $this->createMock(StreamReader::class);
        $eventStore->expects($this->never())->method('retrieveByFilter');

        $repository = new SnapshotRepository($inner, $snapshots, $eventStore, self::liveStreams(), SnapshotArticle::class, ArticleId::class, 'article');

        $this->assertSame($fallback, $repository->retrieve($id));
    }

    #[Test]
    public function a_snapshot_hit_restores_without_the_inner_repository(): void
    {
        // the whole point of the snapshot path: a hit never pays the full replay through the inner
        // repository; only the post-snapshot tail is read from the event store.
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tail());

        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->never())->method('retrieve');

        $repository = new SnapshotRepository($inner, $snapshots, $eventStore, self::liveStreams(), SnapshotArticle::class, ArticleId::class, 'article');

        $article = $repository->retrieve($id);

        $this->assertInstanceOf(SnapshotArticle::class, $article);
        $this->assertSame('Hello', $article->title());
        $this->assertSame(1, $article->version());
    }

    #[Test]
    public function a_snapshot_hit_replays_the_post_snapshot_tail(): void
    {
        // publishing recorded after the sweep, so the hit replays the tail on top
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tail(new EventRecord(
            new Message(new ArticlePublished($id->toString()), [Header::AggregateVersion->key() => 2]),
            SequencePosition::fromInt(2),
            PointInTime::from('2024-01-01T10:00:01.000000+00:00'),
        )));

        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->never())->method('retrieve');

        $repository = new SnapshotRepository($inner, $snapshots, $eventStore, self::liveStreams(), SnapshotArticle::class, ArticleId::class, 'article');

        $article = $repository->retrieve($id);

        $this->assertInstanceOf(SnapshotArticle::class, $article);
        $this->assertTrue($article->published);
        $this->assertSame(2, $article->version());
    }

    #[Test]
    public function a_stale_snapshot_shape_falls_back_to_full_reconstitution(): void
    {
        // fromSnapshot discarded the stale shape, returning null, so the repository falls back to a full replay
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            5,
            ['_snapshot_version' => 99, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tail());

        $fallback = SnapshotArticle::draft($id, 'from events');
        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->once())->method('retrieve')->with($id)->willReturn($fallback);

        $repository = new SnapshotRepository($inner, $snapshots, $eventStore, self::liveStreams(), SnapshotArticle::class, ArticleId::class, 'article');

        $this->assertSame($fallback, $repository->retrieve($id));
    }

    #[Test]
    public function retrieve_wraps_a_non_event_in_the_snapshot_tail_as_a_storage_failure(): void
    {
        // the snapshot fast-path must honor the same port contract as the inner full read: a non-event
        // row in the post-snapshot tail is a corrupt read, surfaced as the contracted StorageFailure,
        // cause preserved, not leaked as the raw Chronicler-internal NotADomainEvent.
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tail(new EventRecord(
            new Message(new stdClass), // a non-event in the post-snapshot tail
            SequencePosition::fromInt(2),
            PointInTime::from('2024-01-01T10:00:01.000000+00:00'),
        )));

        $repository = new SnapshotRepository(
            $this->createStub(AggregateRepository::class),
            $snapshots,
            $eventStore,
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        try {
            $repository->retrieve($id);
            $this->fail('Expected the non-event tail row to surface as a StorageFailure.');
        } catch (StorageFailure $failure) {
            $this->assertInstanceOf(NotADomainEvent::class, $failure->getPrevious());
        }
    }

    #[Test]
    public function retrieve_wraps_a_dbal_read_failure_in_the_snapshot_tail_as_a_storage_failure(): void
    {
        // the EventStore read surfaces raw Doctrine DBAL failures that the abstract port does not
        // contract; the snapshot fast-path catches and wraps them into the contracted StorageFailure,
        // cause preserved, so the DBAL catch arm is genuinely live, not the dead code static analysis sees.
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $dbal = new class('read failed') extends RuntimeException implements DbalException {};

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willThrowException($dbal);

        $repository = new SnapshotRepository(
            $this->createStub(AggregateRepository::class),
            $snapshots,
            $eventStore,
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        try {
            $repository->retrieve($id);
            $this->fail('Expected the DBAL read failure to surface as a StorageFailure.');
        } catch (StorageFailure $failure) {
            $this->assertSame($dbal, $failure->getPrevious());
        }
    }

    #[Test]
    public function retrieve_wraps_a_dbal_snapshot_load_failure_as_a_storage_failure(): void
    {
        // the snapshot load is a read too: a DBAL failure there must cross the port as the contracted
        // StorageFailure, cause preserved, not leak raw before the tail-read try block.
        $id = ArticleId::generate();

        $dbal = new class('load failed') extends RuntimeException implements DbalException {};

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willThrowException($dbal);

        $repository = new SnapshotRepository(
            $this->createStub(AggregateRepository::class),
            $snapshots,
            $this->createStub(StreamReader::class),
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        try {
            $repository->retrieve($id);
            $this->fail('Expected the snapshot load failure to surface as a StorageFailure.');
        } catch (StorageFailure $failure) {
            $this->assertSame($dbal, $failure->getPrevious());
        }
    }

    #[Test]
    #[Group('adversarial')]
    public function retrieve_wraps_every_failure_allowed_by_the_snapshot_store_contract(): void
    {
        // SnapshotStore::load() reserves Throwable for a storage failure, so a conforming non-DBAL
        // adapter's failure crosses the repository port wrapped, never raw
        $failure = new RuntimeException('third-party snapshot adapter failed');
        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willThrowException($failure);

        $repository = new SnapshotRepository(
            $this->createStub(AggregateRepository::class),
            $snapshots,
            $this->createStub(StreamReader::class),
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        try {
            $repository->retrieve(ArticleId::generate());
            $this->fail('Expected the snapshot adapter failure to cross as StorageFailure.');
        } catch (StorageFailure $wrapped) {
            $this->assertSame($failure, $wrapped->getPrevious());
        }
    }

    #[Test]
    public function retrieve_wraps_a_corrupt_stored_position_in_the_snapshot_tail_as_a_storage_failure(): void
    {
        // the fast-path tail read honors the same port contract as the inner full read: a corrupt
        // stored position surfaces as StorageFailure, never as the Chronicler-internal InvalidPosition.
        $corruptPosition = InvalidPosition::notAPositiveInteger('0');

        $failure = $this->tailFailureOf($corruptPosition);

        $this->assertSame($corruptPosition, $failure->getPrevious());
    }

    #[Test]
    public function retrieve_wraps_a_tail_payload_that_cannot_be_deserialized_as_a_storage_failure(): void
    {
        $undeserializable = SerializationException::cannotDeserialize('article.published');

        $failure = $this->tailFailureOf($undeserializable);

        $this->assertSame($undeserializable, $failure->getPrevious());
    }

    #[Test]
    public function retrieve_wraps_an_unparseable_point_in_time_in_the_snapshot_tail_as_a_storage_failure(): void
    {
        $unparseableClock = new ClockException('the stored point in time is not parseable');

        $failure = $this->tailFailureOf($unparseableClock);

        $this->assertSame($unparseableClock, $failure->getPrevious());
    }

    /**
     * Drives retrieve() on a snapshot hit whose post-snapshot tail fails, and returns the failure.
     *
     * The reader hydrates lazily: a corrupt tail row raises while the stream is being consumed, not
     * when `retrieveByFilter()` is called; failing from inside the generator reproduces that, and pins
     * that the try block spans the consumption of the tail, not merely the call that opens it.
     */
    private function tailFailureOf(Throwable $readFailure): StorageFailure
    {
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::failingTail($readFailure));

        $repository = new SnapshotRepository(
            $this->createStub(AggregateRepository::class),
            $snapshots,
            $eventStore,
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        try {
            $repository->retrieve($id);
        } catch (StorageFailure $failure) {
            return $failure;
        }

        $this->fail(sprintf('Expected the %s tail failure to surface as a StorageFailure.', $readFailure::class));
    }

    private static function tail(EventRecord ...$records): Generator
    {
        yield from $records;
    }

    /**
     * @return Generator<int, EventRecord>
     */
    private static function failingTail(Throwable $readFailure): Generator
    {
        yield from [];

        throw $readFailure;
    }

    #[Test]
    #[Group('adversarial')]
    public function an_orphaned_snapshot_never_resurrects_a_deleted_aggregate(): void
    {
        // a deleted stream leaves its snapshot row behind, which is why pruneOrphans exists: the tail
        // is empty, so nothing proved the authoritative history still exists; without the guard, the
        // aggregate would come back at the snapshot version from PURE cache where a full replay says null
        $id = ArticleId::generate();

        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->once())->method('retrieve')->with($id)->willReturn(null);

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            42,
            ['_snapshot_version' => 1, 'title' => 'Ghost', 'published' => true],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tailSince(42)); // empty tail

        $repository = new SnapshotRepository($inner, $snapshots, $eventStore, self::liveStreams(0), SnapshotArticle::class, ArticleId::class, 'article');

        $this->assertNull($repository->retrieve($id));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_recreated_id_is_never_served_the_old_lifes_snapshot(): void
    {
        // the RECREATED id: the stream exists AGAIN, erased then re-created, living a new, shorter life
        // at head 1 while the old snapshot sits at v42; existence alone would serve the DEAD history;
        // the guard probes the head VERSION, sees it below the snapshot, and replays the new truth
        $id = ArticleId::generate();
        $truth = SnapshotArticle::draft($id, 'NEW-LIFE truth');

        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->once())->method('retrieve')->with($id)->willReturn($truth);

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            42,
            ['_snapshot_version' => 1, 'title' => 'OLD-LIFE', 'published' => true],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tailSince(42)); // empty tail after v42

        $repository = new SnapshotRepository($inner, $snapshots, $eventStore, self::liveStreams(1), SnapshotArticle::class, ArticleId::class, 'article');

        $this->assertSame($truth, $repository->retrieve($id));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_snapshot_at_the_live_head_is_served_from_cache_without_a_full_replay(): void
    {
        // the coherence boundary the recreation guard must NOT trip on: an empty tail with the head
        // sitting EXACTLY at the snapshot version proves the authoritative stream still agrees, so the
        // snapshot instance is served and the inner full replay never runs. head < version is the
        // recreation, a shorter new life to discard; head == version is health. The strict `<` is the
        // line between them, and a `<=` here would needlessly replay every in-sync snapshot.
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            5,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tailSince(5)); // empty tail, head at v5

        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->never())->method('retrieve'); // served from cache, no full replay

        $repository = new SnapshotRepository($inner, $snapshots, $eventStore, self::liveStreams(5), SnapshotArticle::class, ArticleId::class, 'article');

        $article = $repository->retrieve($id);

        $this->assertInstanceOf(SnapshotArticle::class, $article);
        $this->assertSame(5, $article->version());
        $this->assertSame('Hello', $article->title());
    }

    #[Test]
    public function an_invalid_snapshot_state_is_a_cache_miss_not_a_read_failure(): void
    {
        // the aggregate's own strict restoreState refused the bag via the InvalidSnapshotState contract:
        // a corrupt CACHE row while the authoritative history is sound; discard, full replay
        $id = ArticleId::generate();
        $replayed = SnapshotArticle::draft($id, 'From Events');

        $inner = $this->createMock(AggregateRepository::class);
        $inner->expects($this->once())->method('retrieve')->with($id)->willReturn($replayed);

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            PickySnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'published' => false], // title MISSING, Picky refuses
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tailSince(1));

        $repository = new SnapshotRepository($inner, $snapshots, $eventStore, self::liveStreams(), PickySnapshotArticle::class, ArticleId::class, 'article');

        $this->assertSame($replayed, $repository->retrieve($id));
    }

    #[Test]
    #[Group('adversarial')]
    public function a_gap_in_the_snapshot_tail_is_corruption_not_state(): void
    {
        // seeded continuity: the tail must run from snapshotVersion + 1; a record jumping ahead means
        // events are missing between the snapshot and it; deciding on that state would pass the CAS
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tailSince(1, new EventRecord(
            new Message(new ArticlePublished($id->toString()), [Header::AggregateVersion->key() => 3]), // gap: expected 2
            SequencePosition::fromInt(3),
            PointInTime::from('2024-01-01T10:00:01.000000+00:00'),
        )));

        $repository = new SnapshotRepository(
            $this->createStub(AggregateRepository::class),
            $snapshots,
            $eventStore,
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        $this->expectException(CorruptStreamHistory::class);
        // the tail is seeded at the snapshot version 1, so version 1 + 1 was expected before the gap
        $this->expectExceptionMessageIsOrContains('expected version 2, observed 3');

        $repository->retrieve($id);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_tail_record_without_a_version_header_is_corruption(): void
    {
        // accepting an unheadered tail record would advance the STATE while freezing the OCC token at
        // the snapshot, state newer than its token; the record is refused where it is visible
        $id = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tailSince(1, new EventRecord(
            new Message(new ArticlePublished($id->toString())), // no AggregateVersion header
            SequencePosition::fromInt(2),
            PointInTime::from('2024-01-01T10:00:01.000000+00:00'),
        )));

        $repository = new SnapshotRepository(
            $this->createStub(AggregateRepository::class),
            $snapshots,
            $eventStore,
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        $this->expectException(CorruptStreamHistory::class);
        // the tail is seeded at the snapshot version 1, so version 1 + 1 was expected for the record
        $this->expectExceptionMessageIsOrContains('version 2 was expected');

        $repository->retrieve($id);
    }

    #[Test]
    #[Group('adversarial')]
    public function a_foreign_event_in_the_snapshot_tail_is_corruption(): void
    {
        // the tail-replay mirror of the full-read foreign-event guard: a tail record whose own aggregate
        // id names ANOTHER aggregate would launder foreign state onto this one; refused where it is visible.
        $id = ArticleId::generate();
        $other = ArticleId::generate();

        $snapshots = $this->createStub(SnapshotStore::class);
        $snapshots->method('load')->willReturn(new Snapshot(
            'article-'.$id->toString(),
            SnapshotArticle::class,
            1,
            ['_snapshot_version' => 1, 'title' => 'Hello', 'published' => false],
            PointInTime::from('2024-01-01T10:00:00.000000+00:00'),
        ));

        $eventStore = $this->createStub(StreamReader::class);
        $eventStore->method('retrieveByFilter')->willReturn(self::tailSince(1, new EventRecord(
            new Message(new ArticlePublished($other->toString()), [Header::AggregateVersion->key() => 2]), // foreign id, contiguous version
            SequencePosition::fromInt(2),
            PointInTime::from('2024-01-01T10:00:01.000000+00:00'),
        )));

        $repository = new SnapshotRepository(
            $this->createStub(AggregateRepository::class),
            $snapshots,
            $eventStore,
            self::liveStreams(),
            SnapshotArticle::class,
            ArticleId::class,
            'article',
        );

        $this->expectException(CorruptStreamHistory::class);

        $repository->retrieve($id);
    }

    /**
     * A StreamHeadStore whose `lastVersion` answer is fixed: PHP_INT_MAX, the default, keeps every
     * healthy path coherent; 0 simulates the erased stream behind an orphaned snapshot row; a small
     * positive value simulates the RECREATED id whose new life sits below the old snapshot.
     */
    private static function liveStreams(int $lastVersion = PHP_INT_MAX): StreamHeadStore
    {
        return new readonly class($lastVersion) implements StreamHeadStore
        {
            public function __construct(private int $lastVersion) {}

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
                return $this->lastVersion;
            }

            public function lockForErase(string $stream): void {}

            public function delete(string $stream): bool
            {
                return false;
            }
        };
    }

    /**
     * A snapshot-tail generator with the seeded return-version protocol: yields the records, returns
     * the last header version, or the seed when empty.
     *
     * @return Generator<int, EventRecord>
     */
    private static function tailSince(int $seed, EventRecord ...$records): Generator
    {
        $version = $seed;
        foreach ($records as $record) {
            $version = $record->message->aggregateVersion() ?? $version;
            yield $record;
        }

        return $version;
    }
}
