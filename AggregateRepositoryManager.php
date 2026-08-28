<?php

declare(strict_types=1);

namespace Storm\AggregateRepository;

use Storm\AggregateRepository\Exception\UnknownAggregate;
use Storm\AggregateRepository\Snapshot\SnapshotStore;
use Storm\Chronicler\Directory\StreamHeadStore;
use Storm\Chronicler\Store\DecisionAppend;
use Storm\Chronicler\Store\StreamReader;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Aggregate\AggregateRepository;
use Storm\Contracts\Aggregate\AggregateRoot;
use Storm\Contracts\Aggregate\SnapshotableAggregateRoot;
use Storm\Message\MessageEnricher;
use Symfony\Component\DependencyInjection\Attribute\Autowire;

/**
 * Resolves and caches one `AggregateRepository` per aggregate type from the `storm.aggregates`
 * config; each entry is an identity class, a stream category, and an optional snapshot block this
 * manager carries without reading, since implementing `SnapshotableAggregateRoot` is what makes a
 * repository snapshot-backed, and the block itself tunes the sweep.
 *
 * Handlers call `for(Order::class)` instead of wiring a repository by hand. `DefaultAggregateRepository`
 * is not autowired since it needs the aggregate class, the id class, and the category, so this manager
 * is the single config-driven build point.
 */
final class AggregateRepositoryManager
{
    /** @var array<class-string, AggregateRepository<AggregateIdentity, AggregateRoot<AggregateIdentity>>> bounded by the configured `storm.aggregates`, one entry per aggregate type, so the only mutable state of the module cannot grow past its configuration */
    private array $repositories = [];

    /**
     * @param  array<class-string, array{id: class-string<AggregateIdentity>, category: string, snapshot?: array{threshold: int, max_age_seconds?: int|null, min_interval_seconds?: int|null}}>  $aggregates
     */
    public function __construct(
        #[Autowire('%storm.aggregates%')]
        private readonly array $aggregates,
        private readonly StreamReader $streamReader,
        private readonly DecisionAppend $decisionAppend,
        private readonly MessageEnricher $enricher,
        private readonly SnapshotStore $snapshots,
        private readonly StreamHeadStore $heads,
    ) {}

    /**
     * @template TId of AggregateIdentity
     * @template T of AggregateRoot<TId>
     *
     * @param  class-string<T>  $aggregateClass
     * @return AggregateRepository<TId, T>
     *
     * @throws UnknownAggregate when the class is not declared under `storm.aggregates`.
     */
    public function for(string $aggregateClass): AggregateRepository
    {
        if (! isset($this->repositories[$aggregateClass])) {
            $this->repositories[$aggregateClass] = $this->build($aggregateClass);
        }

        /** @var AggregateRepository<TId, T> */
        return $this->repositories[$aggregateClass];
    }

    /**
     * @param  class-string<AggregateRoot<AggregateIdentity>>  $aggregateClass
     * @return AggregateRepository<AggregateIdentity, AggregateRoot<AggregateIdentity>>
     *
     * @throws UnknownAggregate when the class is not declared under `storm.aggregates`.
     */
    private function build(string $aggregateClass): AggregateRepository
    {
        if (! isset($this->aggregates[$aggregateClass])) {
            throw UnknownAggregate::notConfigured($aggregateClass);
        }

        $config = $this->aggregates[$aggregateClass];

        $repository = new DefaultAggregateRepository(
            $aggregateClass,
            $config['id'],
            $config['category'],
            $this->streamReader,
            $this->decisionAppend,
            $this->enricher,
        );

        // Implementing SnapshotableAggregateRoot is the opt-in: wrap for snapshot-accelerated reads.
        if (is_subclass_of($aggregateClass, SnapshotableAggregateRoot::class)) {
            /** @var class-string<SnapshotableAggregateRoot<AggregateIdentity>> $aggregateClass */
            return new SnapshotRepository($repository, $this->snapshots, $this->streamReader, $this->heads, $aggregateClass, $config['id'], $config['category']);
        }

        return $repository;
    }
}
