<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Exception;

use InvalidArgumentException;

/**
 * Raised when a historical inspection targets a version the inspector refuses to fold: not
 * positive, or past its replay ceiling.
 *
 * An authoring guardrail rather than a stored-data alarm: a caller serving operators is expected to
 * refuse the same value first, in its own words, before reaching the inspector.
 */
final class InspectionVersionOutOfRange extends InvalidArgumentException
{
    public static function notPositive(string $aggregateClass, int $version): self
    {
        return new self(sprintf(
            'Cannot inspect %s at version %d: an aggregate version starts at 1.',
            $aggregateClass,
            $version,
        ));
    }

    public static function pastCeiling(string $aggregateClass, int $version, int $ceiling): self
    {
        return new self(sprintf(
            'Cannot inspect %s at version %d: a historical inspection folds at most %d versions.',
            $aggregateClass,
            $version,
            $ceiling,
        ));
    }
}
