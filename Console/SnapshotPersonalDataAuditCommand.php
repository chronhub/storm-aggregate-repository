<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Console;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Storm\AggregateRepository\Snapshot\PersonalDataSnapshotGuard;
use Storm\Support\Console\PositiveIntOption;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Reports metadata for snapshots at or after the first currently recognized personal event.
 *
 * One read-only, repeatable-read transaction covers each page. The limit bounds examined snapshots
 * and reported candidates, with one additional metadata row to determine whether a next page exists.
 * Each statement has a timeout; there is no end-to-end time or I/O bound. The stream-version index
 * supports the event probe but sparse marked types can require scanning the entire stream.
 *
 * Examples:
 *
 * ```bash
 * bin/console storm:snapshot:audit-personal
 * ```
 * ```bash
 * bin/console storm:snapshot:audit-personal --limit=50 --after=account-42 --timeout-ms=2000
 * ```
 */
#[AsCommand(name: 'storm:snapshot:audit-personal', description: 'Report historical snapshot candidates without reading payloads or modifying data.')]
final class SnapshotPersonalDataAuditCommand extends Command
{
    public function __construct(private readonly Connection $connection, private readonly PersonalDataSnapshotGuard $guard)
    {
        parent::__construct();
    }

    protected function configure(): void
    {
        $this
            ->addOption('limit', null, InputOption::VALUE_REQUIRED, 'Maximum snapshots examined and candidates reported in this page, 1 to 1000.', '100')
            ->addOption('after', null, InputOption::VALUE_REQUIRED, 'Resume strictly after this stream using the previous report next_after.')
            ->addOption('timeout-ms', null, InputOption::VALUE_REQUIRED, 'PostgreSQL timeout per statement in milliseconds, 1 to 60000.', '1000');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $limit = PositiveIntOption::parse($input->getOption('limit'));
        $timeout = PositiveIntOption::parse($input->getOption('timeout-ms'));
        $after = $input->getOption('after');
        if ($limit === null || $limit > 1000 || $timeout === null || $timeout > 60000 || ($after !== null && (! is_string($after) || str_contains($after, "\0")))) {
            $output->writeln('Invalid audit options: --limit must be 1..1000, --timeout-ms must be 1..60000, and --after must be text without NUL.');

            return self::INVALID;
        }

        if ($this->connection->isTransactionActive()) {
            $output->writeln('The audit requires its own read-only transaction; an existing transaction is active.');

            return self::FAILURE;
        }

        $this->connection->beginTransaction();
        try {
            $this->connection->executeStatement('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY');
            $this->connection->executeStatement("SET LOCAL statement_timeout = '".$timeout."ms'");
            $rows = $this->connection->fetchAllAssociative(
                'SELECT stream, version, created_at FROM snapshots'.($after === null ? '' : ' WHERE stream > :after').' ORDER BY stream LIMIT '.($limit + 1),
                $after === null ? [] : ['after' => $after],
            );
            $hasMore = count($rows) > $limit;
            $rows = array_slice($rows, 0, $limit);
            $candidates = [];
            $lastStream = null;
            foreach ($rows as $row) {
                $lastStream = (string) $row['stream'];
                $first = $this->guard->firstMarkedVersion($lastStream);
                if ($first !== null && (int) $row['version'] >= $first) {
                    $candidates[] = [
                        'stream' => $lastStream,
                        'snapshot_version' => (int) $row['version'],
                        'first_marked_version' => $first,
                        'created_at' => (string) $row['created_at'],
                    ];
                }
            }
        } catch (Exception) {
            $output->writeln('Audit failed or exceeded its PostgreSQL statement timeout. No report was emitted; retry the same cursor.');

            return self::FAILURE;
        } finally {
            $this->connection->rollBack();
        }

        $output->writeln(json_encode([
            'candidates' => $candidates,
            'examined' => count($rows),
            'has_more' => $hasMore,
            'next_after' => $hasMore ? $lastStream : null,
            'limitations' => [
                'Candidates do not prove clear personal data is present in snapshot state. No candidates does not certify clean state.',
                'Unknown renamed aliases, a changed or incomplete Personal map, and erased events cannot be individually classified from the current history.',
                'Only metadata is read. Payloads are neither inspected nor reported.',
                'Each page is consistent; separate pages can miss concurrent inserts or changes behind the cursor. They are not a global point-in-time inventory.',
                'The limit bounds examined snapshots and output, not event rows scanned or PostgreSQL I/O. The timeout applies per statement, not to the entire audit.',
                'Long pages retain an MVCC visibility horizon and can delay removal of dead tuples by vacuum.',
            ],
        ], JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT), OutputInterface::OUTPUT_RAW);

        return self::SUCCESS;
    }
}
