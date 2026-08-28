<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Tests\Fixture;

use Storm\Aggregate\ProvideAggregateIdentity;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Symfony\Component\Uid\Uuid;

/**
 * A typed aggregate identity, used to exercise the identity contract + trait.
 */
final readonly class ArticleId implements AggregateIdentity
{
    use ProvideAggregateIdentity;

    public static function generate(): static
    {
        return new self(Uuid::v7());
    }
}
