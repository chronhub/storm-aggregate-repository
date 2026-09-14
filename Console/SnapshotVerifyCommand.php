<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Console;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use Generator;
use JsonException;
use Override;
use Storm\AggregateRepository\AggregateHistoryReplay;
use Storm\Chronicler\Store\StreamReader;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Aggregate\SnapshotableAggregateRoot;
use Storm\Stream\StreamName;
use Storm\Support\Console\PositiveIntOption;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Throwable;

/**
 * Reads the lie the snapshot guarantees leave uncovered: a snapshot whose state contradicts the
 * stream at a version the head still holds, which the head probe cannot see and which the
 * repository restores and replays the tail onto.
 *
 * For a sample of snapshots, the stream is refolded from its first event to the snapshot's
 * version, the aggregate's own `toSnapshot()` is compared to the stored state, and every
 * differing key is named. An orphan, a snapshot whose stream head is gone, is counted and not
 * refolded, there being no stream to refold, `storm:snapshot:prune-orphans` being its verb. The
 * command reads and changes nothing; a lying snapshot is the operator's to discard, and the sweep
 * retakes it.
 *
 * The sample walks the snapshots in stream order, `--after` naming the last stream of the previous
 * run so a scheduled audit covers the table over its runs instead of the same first page; the
 * last stream read is printed for that purpose. `--category` narrows to the key range of one
 * category, a dash inside the category included; a category extending another past a dash sorts
 * inside the shorter one's range, as it does for the sweep. The comparison is canonical, keys sorted and a
 * whole float read as its integer; a list keeps its order, since `toSnapshot()` writes the state
 * the repository restores and an order that varies between two folds is already a snapshot that
 * lies to its own restore. `_snapshot_version` is left out, a stale one being invalidated on load.
 *
 * Examples:
 *
 * ```bash
 * bin/console storm:snapshot:verify
 * ```
 *
 * ```bash
 * bin/console storm:snapshot:verify --category=account --sample=50
 * ```
 *
 * ```bash
 * bin/console storm:snapshot:verify --category=account --after=account-01900000-0000-7000-8000-0000000000de
 * ```
 *
 * ```bash
 * bin/console storm:snapshot:verify --json
 * ```
 */
#[AsCommand(name: 'storm:snapshot:verify', description: 'Refold a sample of snapshotted streams to their snapshot version and name every snapshot whose state lies.')]
final class SnapshotVerifyCommand extends Command
{
    /**
     * @param  array<class-string, array{id: class-string<AggregateIdentity>, category: string}>  $aggregates
     */
    public function __construct(
        private readonly Connection $connection,
        private readonly StreamReader $streamReader,
        #[Autowire('%storm.aggregates%')]
        private readonly array $aggregates,
    ) {
        parent::__construct();
    }

    #[Override]
    protected function configure(): void
    {
        $this->addOption('category', null, InputOption::VALUE_REQUIRED, 'Verify the snapshots of one category only');
        $this->addOption('sample', null, InputOption::VALUE_REQUIRED, 'Snapshots verified per run, in stream order', '20');
        $this->addOption('after', null, InputOption::VALUE_REQUIRED, 'Resume after this stream, the last one printed by the previous run');
        $this->addOption('json', null, InputOption::VALUE_NONE, 'Print the machine-readable verdicts');
    }

    /**
     * @throws JsonException when the machine document cannot be encoded
     */
    #[Override]
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $sample = PositiveIntOption::parse($input->getOption('sample'));
        if ($sample === null) {
            $io->error('--sample must be a positive integer.');

            return Command::INVALID;
        }
        $category = $input->getOption('category');
        $category = is_string($category) && $category !== '' ? $category : null;
        $after = $input->getOption('after');
        $after = is_string($after) && $after !== '' ? $after : null;
        $json = $input->getOption('json') === true;

        // the category filter is a KEY RANGE on the primary key, [category-, category.), `.` being
        // the successor byte of `-`, read in bytes under the column's C collation: one contiguous
        // index slice, a dash inside the category included; the cursor seeds the lower bound
        $lower = $category === null ? '' : $category.'-';
        if ($after !== null && $after > $lower) {
            $lower = $after;
        }

        /** @var list<array{stream: string, aggregate_type: string, version: int|string, state: string, orphaned: bool}> $rows */
        $rows = $this->connection->fetchAllAssociative(
            /* language=PostgreSQL */
            'SELECT s.stream, s.aggregate_type, s.version, s.state, (h.stream IS NULL) AS orphaned
             FROM snapshots s LEFT JOIN stream_heads h ON h.stream = s.stream
             WHERE s.stream > :lower'
            .($category === null ? '' : ' AND s.stream < :upper')
            .' ORDER BY s.stream LIMIT :sample',
            ['lower' => $lower, 'sample' => $sample] + ($category === null ? [] : ['upper' => $category.'.']),
            ['sample' => ParameterType::INTEGER],
        );
        $last = $rows === [] ? null : array_last($rows)['stream'];

        $verified = 0;
        $orphaned = 0;
        $lying = [];
        foreach ($rows as $row) {
            if ($row['orphaned']) {
                $orphaned++;

                continue;
            }
            $verdict = $this->verify($row['stream'], $row['aggregate_type'], (int) $row['version'], (string) $row['state']);
            if ($verdict === null) {
                $verified++;
            } else {
                $lying[] = ['stream' => $row['stream'], 'version' => (int) $row['version'], 'reason' => $verdict];
            }
        }

        if ($json) {
            $output->writeln(json_encode(['verified' => $verified, 'lying' => $lying, 'orphaned' => $orphaned, 'last' => $last], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
        } else {
            if ($lying !== []) {
                $io->table(['Stream', 'Version', 'Lie'], array_map(static fn (array $lie): array => [$lie['stream'], (string) $lie['version'], $lie['reason']], $lying));
            }
            $line = sprintf('%d verified, %d lying, %d orphaned (sample of %d%s).', $verified, count($lying), $orphaned, $sample, $last === null ? '' : ', last '.$last);
            $lying === [] ? $io->success($line) : $io->error($line.' A lying snapshot is discarded by hand; the sweep retakes it.');
        }

        return $lying === [] ? Command::SUCCESS : Command::FAILURE;
    }

    /**
     * The stream refolded to the snapshot's version against the stored state: null when they agree,
     * the reason otherwise. A refold that fails names its failure rather than passing.
     */
    private function verify(string $stream, string $aggregateType, int $version, string $state): ?string
    {
        if (! class_exists($aggregateType) || ! is_subclass_of($aggregateType, SnapshotableAggregateRoot::class)) {
            return sprintf('aggregate type %s is unknown or not snapshotable', $aggregateType);
        }
        $config = $this->aggregates[$aggregateType] ?? null;
        if ($config === null) {
            return sprintf('aggregate type %s is not configured under storm.aggregates', $aggregateType);
        }
        try {
            $id = $config['id']::fromString((string) new StreamName($stream)->qualifier);
            $aggregate = $aggregateType::reconstitute($id, AggregateHistoryReplay::validated($aggregateType, $id, $this->upTo($stream, $version), 0));
        } catch (Throwable $e) {
            return sprintf('the refold failed (%s)', $e::class);
        }
        if ($aggregate === null || $aggregate->version() !== $version) {
            return sprintf('the stream holds %d event(s) below the snapshot version', $aggregate?->version() ?? 0);
        }
        /** @var array<string, mixed> $stored */
        $stored = json_decode($state, true, 512, JSON_THROW_ON_ERROR);
        unset($stored['_snapshot_version']);
        $folded = $aggregate->toSnapshot();
        unset($folded['_snapshot_version']);
        $differing = [];
        foreach (array_unique([...array_keys($stored), ...array_keys($folded)]) as $key) {
            if (! array_key_exists($key, $stored) || ! array_key_exists($key, $folded) || $this->canonical($stored[$key]) !== $this->canonical($folded[$key])) {
                $differing[] = (string) $key;
            }
        }

        return $differing === [] ? null : 'state differs from the refold on '.implode(', ', $differing);
    }

    /**
     * The stream's records up to and including the snapshot's version, read lazily and left there.
     *
     * @return Generator<mixed>
     */
    private function upTo(string $stream, int $version): Generator
    {
        foreach ($this->streamReader->retrieveAll(new StreamName($stream)) as $record) {
            if ($record->message->aggregateVersion() > $version) {
                break;
            }
            yield $record;
        }
    }

    private function canonical(mixed $value): string
    {
        if (is_array($value)) {
            ksort($value);
            $value = array_map($this->canonical(...), $value);
        } elseif (is_float($value) && floor($value) === $value && is_finite($value)) {
            // a whole float and its integer are one number on the wire: 20.0 stored, 20 refolded
            $value = (int) $value;
        }

        return json_encode($value, JSON_THROW_ON_ERROR);
    }
}
