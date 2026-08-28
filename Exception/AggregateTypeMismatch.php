<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Exception;

use InvalidArgumentException;
use Storm\Contracts\Aggregate\AggregateTypeMismatch as AggregateTypeMismatchContract;

/**
 * Raised when an aggregate handed to a repository does not match the aggregate type that
 * repository is bound to; a configuration or wiring error.
 */
final class AggregateTypeMismatch extends InvalidArgumentException implements AggregateTypeMismatchContract
{
    public static function id(string $expected, string $actual): self
    {
        return new self(sprintf(
            'Aggregate identity mismatch: this repository expects %s, got %s.',
            $expected,
            $actual,
        ));
    }

    public static function aggregate(string $expected, string $actual): self
    {
        return new self(sprintf(
            'Aggregate type mismatch: this repository is bound to %s, got %s.',
            $expected,
            $actual,
        ));
    }
}
