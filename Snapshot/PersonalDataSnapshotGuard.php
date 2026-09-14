<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Snapshot;

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Storm\AggregateRepository\Console\SnapshotSweepCommand;
use Storm\Contracts\Chronicler\EventTypeMapper;
use Storm\Stream\Exception\InvalidStreamException;
use Storm\Stream\StreamName;

/**
 * The snapshot exclusion of crypto-shredding.
 *
 * An aggregate whose stream folds a `#[Personal]` event must NOT be snapshotted: the fold sees
 * decrypted values, so a snapshot would persist the subject's personal state in clear, beside events
 * whose whole protection is being ciphered, and no forget could reach it. The mapping from subject
 * to streams is not declarative, so nobody knows which snapshots to destroy. Exclusion is the only
 * safe design; the price of PII in state is full replay.
 *
 * The check is per STREAM, against what is actually stored: one indexed probe over the stream's
 * rows for any alias a marked class may be stored under, current or former, via `storedTypesOf()`.
 * So a marked class anywhere in the app refuses exactly the streams that carry it, and no
 * configuration link from aggregate to event classes needs to exist.
 *
 * Snapshot ENCRYPTION is the designed successor, a second declaration for state keys, triggered by the first PII-bearing aggregate whose replay measurably hurts; the sweep knows how
 * to measure.
 *
 * @see SnapshotSweepCommand the only snapshot producer, where the refusal lands
 */
final readonly class PersonalDataSnapshotGuard
{
    public function __construct(
        private Connection $connection,
        private EventTypeMapper $mapper,
        /**
         * The compiled `storm.personal_data` map; the bundle overrides the empty default with the
         * `#[Personal]` scan product. An empty map means no class is marked and every stream is snapshot-clean.
         *
         * @var array<class-string, array{subject: string, keys: list<string>, fallbacks: array<string, scalar|null>}>
         */
        private array $map = [],
    ) {}

    /**
     * The stored type alias that makes this stream PII-bearing, or null when the stream is clean.
     *
     * @throws InvalidStreamException when the stream value does not parse under the stream grammar
     * @throws Exception on a DBAL failure probing the stream's stored types
     */
    public function refusal(string $stream): ?string
    {
        if ($this->map === []) {
            return null;
        }

        $marked = [];
        foreach (array_keys($this->map) as $class) {
            // every alias the class may be stored under, former spellings included: an old row
            // under a replaced alias is still the same personal data
            $marked = [...$marked, ...$this->mapper->storedTypesOf($class)];
        }

        // the stream name implies its category, but event_store is partitioned BY LIST (category):
        // the redundant-looking predicate is what lets the planner prune the probe to ONE partition
        $offense = $this->connection->fetchOne(
            /** @lang PostgreSQL */
            'SELECT type FROM event_store WHERE category = :category AND stream = :stream AND type IN (:types) LIMIT 1',
            ['category' => new StreamName($stream)->category, 'stream' => $stream, 'types' => $marked],
            ['types' => ArrayParameterType::STRING],
        );

        return is_string($offense) ? $offense : null;
    }

    /**
     * Earliest stored version recognized by the current personal-data map and its known aliases.
     *
     * A null result cannot rule out erased history or aliases absent from the current map.
     * The category predicate permits partition pruning; the ordered stream index can still scan
     * every event of a stream whose marked types are sparse or absent.
     * Stored names use the SQL schema's first-delimiter category rule without current grammar
     * validation or case normalization, so imported historical spellings remain auditable.
     *
     * @throws Exception on a DBAL failure probing the stream's stored types
     */
    public function firstMarkedVersion(string $stream): ?int
    {
        $marked = [];
        foreach (array_keys($this->map) as $class) {
            $marked = [...$marked, ...$this->mapper->storedTypesOf($class)];
        }

        if ($marked === []) {
            return null;
        }

        $version = $this->connection->fetchOne(
            'SELECT version FROM event_store WHERE category = :category AND stream = :stream AND type IN (:types) ORDER BY version LIMIT 1',
            ['category' => explode('-', $stream, 2)[0], 'stream' => $stream, 'types' => $marked],
            ['types' => ArrayParameterType::STRING],
        );

        return $version === false ? null : (int) $version;
    }
}
