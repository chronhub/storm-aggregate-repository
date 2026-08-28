<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Exception;

use InvalidArgumentException;
use Storm\AggregateRepository\AggregateRepositoryManager;

/**
 * Thrown when `AggregateRepositoryManager::for()` is asked for an aggregate class not declared
 * under `storm.aggregates`; a configuration or setup error.
 *
 * @see AggregateRepositoryManager
 */
final class UnknownAggregate extends InvalidArgumentException
{
    /**
     * @param  class-string  $aggregateClass
     */
    public static function notConfigured(string $aggregateClass): self
    {
        return new self(sprintf(
            'No aggregate repository configured for "%s". Declare it under "storm.aggregates".',
            $aggregateClass,
        ));
    }
}
