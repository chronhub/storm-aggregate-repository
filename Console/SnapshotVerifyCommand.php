<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Console;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\ParameterType;
use JsonException;
use Override;
use Storm\AggregateRepository\HistoricalAggregateInspector;
use Storm\Chronicler\Store\StreamReader;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Aggregate\SnapshotableAggregateRoot;
use Storm\Stream\StreamCategory;
use Storm\Stream\StreamName;
use Storm\Support\Console\PositiveIntOption;
use Storm\Support\Text\Str;
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
        $category = Str::nonEmptyOrNull($input->getOption('category'));
        $after = Str::nonEmptyOrNull($input->getOption('after'));

        $rows = $this->fetchSample($category, $after, $sample);

        $report = ['verified' => 0, 'lying' => [], 'orphaned' => 0, 'last' => $rows === [] ? null : array_last($rows)['stream']];
        foreach ($rows as $row) {
            if ($row['orphaned']) {
                $report['orphaned']++;

                continue;
            }
            $version = (int) $row['version'];
            $lie = $this->verify($row['stream'], $row['aggregate_type'], $version, (string) $row['state']);
            if ($lie === null) {
                $report['verified']++;
            } else {
                $report['lying'][] = ['stream' => $row['stream'], 'version' => $version, 'reason' => $lie];
            }
        }

        if ($input->getOption('json') === true) {
            $output->writeln(json_encode($report, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
        } else {
            $this->printSummary($io, $report, $sample);
        }

        return $report['lying'] === [] ? Command::SUCCESS : Command::FAILURE;
    }

    /**
     * The next page of snapshots in stream order, each flagged when its stream head is gone.
     *
     * @return list<array{stream: string, aggregate_type: string, version: int|string, state: string, orphaned: bool}>
     */
    private function fetchSample(?string $category, ?string $after, int $sample): array
    {
        // a category is the key range [category-, category.). `.` is the byte right after `-`, so
        // the range only holds in bytes: the comparisons and the order name `COLLATE "C"` as the
        // column does, one contiguous index slice on the pinned column and still the right range on
        // a column whose collation has drifted. The join names none, its two columns being a pair:
        // its rows stay right while both drift together or one falls to the database default, and
        // PostgreSQL refuses the query when one side carries another named collation
        $lower = $category === null ? '' : $category.'-';
        // the cursor only moves the lower bound forward; PHP compares these strings bytewise too
        if ($after !== null && $after > $lower) {
            $lower = $after;
        }

        $sql =
            /* language=PostgreSQL */
            'SELECT s.stream, s.aggregate_type, s.version, s.state, (h.stream IS NULL) AS orphaned
             FROM snapshots s LEFT JOIN stream_heads h ON h.stream = s.stream
             WHERE s.stream COLLATE "C" > :lower';
        $params = ['lower' => $lower, 'sample' => $sample];
        if ($category !== null) {
            $sql .= ' AND s.stream COLLATE "C" < :upper';
            $params['upper'] = $category.'.';
        }

        /** @var list<array{stream: string, aggregate_type: string, version: int|string, state: string, orphaned: bool}> */
        return $this->connection->fetchAllAssociative($sql.' ORDER BY s.stream COLLATE "C" LIMIT :sample', $params, ['sample' => ParameterType::INTEGER]);
    }

    /**
     * @param  array{verified: int, lying: list<array{stream: string, version: int, reason: string}>, orphaned: int, last: ?string}  $report
     */
    private function printSummary(SymfonyStyle $io, array $report, int $sample): void
    {
        $lying = $report['lying'];
        if ($lying !== []) {
            $io->table(
                ['Stream', 'Version', 'Lie'],
                array_map(static fn (array $lie): array => [$lie['stream'], (string) $lie['version'], $lie['reason']], $lying),
            );
        }

        $line = sprintf(
            '%d verified, %d lying, %d orphaned (sample of %d%s).',
            $report['verified'],
            count($lying),
            $report['orphaned'],
            $sample,
            $report['last'] === null ? '' : ', last '.$report['last'],
        );
        if ($lying === []) {
            $io->success($line);
        } else {
            $io->error($line.' A lying snapshot is discarded by hand; the sweep retakes it.');
        }
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
            // the repository's own bounded fold up to the snapshot version, lenient on a short
            // stream so the verdict below can name how far the stream still reaches
            $refold = new HistoricalAggregateInspector($aggregateType, $config['id'], new StreamCategory($config['category']), $this->streamReader)
                ->foldUpTo($id, $version);
        } catch (Throwable $e) {
            return sprintf('the refold failed (%s)', $e::class);
        }
        if ($refold === null || $refold->version !== $version) {
            return sprintf('the stream holds %d event(s) below the snapshot version', $refold->version ?? 0);
        }
        try {
            $stored = json_decode($state, true, 512, JSON_THROW_ON_ERROR);
        } catch (JsonException) {
            return 'the stored state is not valid JSON';
        }
        // jsonb holds any JSON value; the store reads anything but a keyed bag as a corrupt row
        if (! is_array($stored) || ($stored !== [] && array_is_list($stored))) {
            return 'the stored state is not a JSON object';
        }
        /** @var array<string, mixed> $stored */
        $differing = $this->differingKeys($stored, $refold->state);

        return $differing === [] ? null : 'state differs from the refold on '.implode(', ', $differing);
    }

    /**
     * @param  array<string, mixed>  $stored
     * @param  array<string, mixed>  $folded
     * @return list<string>
     */
    private function differingKeys(array $stored, array $folded): array
    {
        // a stale `_snapshot_version` is invalidated on load, so it is never a lie
        unset($stored['_snapshot_version'], $folded['_snapshot_version']);

        $differing = [];
        foreach (array_unique([...array_keys($stored), ...array_keys($folded)]) as $key) {
            $same = array_key_exists($key, $stored)
                && array_key_exists($key, $folded)
                && $this->canonical($stored[$key]) === $this->canonical($folded[$key]);
            if (! $same) {
                $differing[] = (string) $key;
            }
        }

        return $differing;
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
