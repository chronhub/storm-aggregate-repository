<?php

declare(strict_types=1);

namespace Storm\AggregateRepository;

/**
 * An aggregate's state as folded at one version, detached from the aggregate that produced it.
 *
 * A read result only: it carries the version the fold reached and the aggregate's own
 * `toSnapshot()` representation, never the `AggregateRoot` instance, so nothing holding it can
 * decide on a past state or hand it back to a repository write.
 */
final readonly class HistoricalAggregateState
{
    /**
     * @param  positive-int  $version
     * @param  array<string, mixed>  $state
     */
    public function __construct(
        public int $version,
        public array $state,
    ) {}
}
