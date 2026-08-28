<?php

declare(strict_types=1);

namespace Storm\AggregateRepository;

use Doctrine\DBAL\Exception;
use Storm\AggregateRepository\Exception\AggregateTypeMismatch;
use Storm\AggregateRepository\Exception\EventAggregateIdMismatch;
use Storm\Chronicler\Exception\NotADomainEvent;
use Storm\Chronicler\Exception\StorageFailure;
use Storm\Chronicler\Store\DecisionAppend;
use Storm\Chronicler\Store\StreamReader;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Aggregate\AggregateRepository;
use Storm\Contracts\Aggregate\AggregateRoot;
use Storm\Contracts\Chronicler\InvalidPosition;
use Storm\Contracts\Clock\ClockExceptionContract;
use Storm\Contracts\Serializer\SerializationExceptionContract;
use Storm\Message\Header;
use Storm\Message\Message;
use Storm\Message\MessageEnricher;
use Storm\Stream\Exception\InvalidStreamException;
use Storm\Stream\Stream;
use Storm\Stream\StreamName;

/**
 * Default `AggregateRepository`, bound to one aggregate type and stream category.
 *
 * `store()` wraps each released event into a `Message` carrying the aggregate headers id, id-type,
 * type, and version; the id-type travels so a pure-transport consumer outside Storm can rebuild the
 * typed id. It runs each message through the `MessageEnricher` chain for message id, type,
 * occurred-at, and causation, then appends under optimistic concurrency with
 * `expectedVersion = version - count`. `retrieve()` replays the stream records through the aggregate's
 * `reconstitute()`, returning null when the stream is empty.
 *
 * Not autowired: an instance needs its aggregate class, id class, and stream category; the
 * config-driven `AggregateRepositoryManager` builds and caches these.
 *
 * @template TId of AggregateIdentity
 * @template T of AggregateRoot<TId>
 *
 * @implements AggregateRepository<TId, T>
 *
 * @see AggregateRepositoryManager
 */
final readonly class DefaultAggregateRepository implements AggregateRepository
{
    /**
     * @param  class-string<T>  $aggregateClass
     * @param  class-string<TId>  $idClass
     */
    public function __construct(
        private string $aggregateClass,
        private string $idClass,
        private string $category,
        private StreamReader $streamReader,
        private DecisionAppend $decisionAppend,
        private MessageEnricher $enricher,
    ) {}

    public function store(AggregateRoot $aggregate): void
    {
        // The identity check below is not enough on its own: two aggregates SHARING an id class,
        // exactly what GenericAggregateIdV7 invites, would pass it, and the wrong aggregate's events
        // would land in this repository's category under this repository's aggregate_type header.
        // instanceof, not exact-class, on purpose: extending an aggregate base is a supported contract;
        // a subclass persists under the BOUND class's aggregate_type header and reconstitutes as the
        // bound class; polymorphic round-tripping of the subtype is not provided.
        if (! $aggregate instanceof $this->aggregateClass) {
            throw AggregateTypeMismatch::aggregate($this->aggregateClass, $aggregate::class);
        }

        $identity = $aggregate->identity();

        if ($identity::class !== $this->idClass) {
            throw AggregateTypeMismatch::id($this->idClass, $identity::class);
        }

        $events = $aggregate->releaseEvents();

        if ($events === []) {
            return;
        }

        // Stream version expected before this append. version() counts every event ever recorded and
        // $events is exactly the unreleased tail, so version() >= count($events) holds by construction
        // since recordThat bumps the version and appends in lockstep; $expected is never negative.
        $expected = $aggregate->version() - count($events);

        $messages = [];
        foreach ($events as $i => $event) {
            // the payload id IS the contract, DomainEvent::aggregateId, a "self-contained event":
            // a mismatch here would persist a fact whose stream and payload name different aggregates
            if ($event->aggregateId() !== $identity->toString()) {
                throw EventAggregateIdMismatch::of($event::class, $identity->toString(), $event->aggregateId());
            }

            $messages[] = $this->enricher->enrich(
                new Message($event, [
                    Header::AggregateId->value => $identity->toString(),
                    Header::AggregateIdType->value => $identity::class,
                    Header::AggregateType->value => $this->aggregateClass,
                    Header::AggregateVersion->value => $expected + $i + 1,
                ]),
            );
        }

        // The write-side mirror of retrieve()'s translation: the raw driver failure stays BELOW the
        // retry decorator that pattern-matches it, and by the time control returns here the retry
        // has run and given up, so wrapping at this port boundary cannot blind it. The concurrency
        // contracts, StaleVersion and DuplicateVersion, are not infra failures and pass distinct.
        try {
            $this->decisionAppend->appendTo(new Stream($this->streamNameFor($identity), $messages), $expected);
        } catch (Exception|SerializationExceptionContract $e) {
            throw StorageFailure::wrap(sprintf('the %s stream append', $this->category), $e);
        }
    }

    public function retrieve(AggregateIdentity $id): ?AggregateRoot
    {
        // the mirror of store()'s check: two aggregates SHARING an id class would otherwise read each
        // other's category with a foreign concrete identity; the template TId only exists in phpdoc,
        // so the repository, which knows $idClass, is where the runtime promise is kept.
        if ($id::class !== $this->idClass) {
            throw AggregateTypeMismatch::id($this->idClass, $id::class);
        }

        // An unknown aggregate yields an empty history, so `reconstitute` returns null at version 0.
        // The infra failures of the read, DBAL and malformed stored JSON / type / position / datetime,
        // and a corrupt stored row that is not a domain event, a NotADomainEvent, are translated here,
        // at the port boundary, into the contracted StorageFailure; the replay's own corruption, the
        // CorruptAggregateHistory contract from the trait's exactness check and AggregateHistoryReplay's
        // continuity checks, and an unknown stored event type both propagate distinct.
        try {
            return ($this->aggregateClass)::reconstitute(
                $id,
                AggregateHistoryReplay::validated($this->aggregateClass, $id, $this->streamReader->retrieveAll($this->streamNameFor($id)), 0),
            );
        } catch (Exception|InvalidPosition|SerializationExceptionContract|ClockExceptionContract|NotADomainEvent $e) {
            throw StorageFailure::wrap(sprintf('the %s stream read', $this->category), $e);
        }
    }

    /**
     * @throws InvalidStreamException when the category / id does not form a valid stream name
     */
    private function streamNameFor(AggregateIdentity $id): StreamName
    {
        return new StreamName($this->category)->withQualifier($id->toString());
    }
}
