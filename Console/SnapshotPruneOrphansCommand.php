<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Console;

use Override;
use Storm\AggregateRepository\Snapshot\SnapshotStore;
use Storm\Support\Console\PositiveIntOption;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Throwable;

/**
 * Prunes orphan snapshots, rows in `snapshots` whose stream no longer exists in `stream_heads`. A
 * snapshot is a cache keyed by stream; `StreamEraser` removes an aggregate's events and its `stream_heads`
 * row but deliberately not its snapshot, since it does not own projector or snapshot state, so an erased
 * aggregate leaves its snapshot behind. This reclaims those; there is no aggregate left to reconstruct
 * from them.
 *
 * Structural, not age-based: the guard is an anti-join on "no matching stream_heads row", so it can only
 * ever remove a snapshot whose source stream is gone, never a live one. Batched and idempotent.
 *
 * `--dry-run` counts without deleting; `--batch` caps each statement so it never holds a long lock.
 *
 * Examples:
 *
 * ```bash
 * bin/console storm:snapshot:prune-orphans
 * ```
 *
 * ```bash
 * bin/console storm:snapshot:prune-orphans --dry-run
 * ```
 *
 * @see SnapshotSweepCommand
 */
#[AsCommand(name: 'storm:snapshot:prune-orphans', description: 'Prune snapshots whose stream no longer exists (orphaned by StreamEraser).')]
final class SnapshotPruneOrphansCommand extends Command
{
    public function __construct(
        private readonly SnapshotStore $snapshots,
    ) {
        parent::__construct();
    }

    #[Override]
    protected function configure(): void
    {
        $this->addOption('batch', null, InputOption::VALUE_REQUIRED, 'Rows deleted per batch', '1000');
        $this->addOption('dry-run', null, InputOption::VALUE_NONE, 'Count what would be pruned, delete nothing');
    }

    /**
     * {@inheritDoc}
     *
     * @throws Throwable on a storage failure of the orphan count or of the prune
     */
    #[Override]
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $batch = PositiveIntOption::parse($input->getOption('batch'));
        if ($batch === null) {
            $io->error('--batch must be a positive integer (orphans deleted per statement), e.g. --batch=1000.');

            return Command::INVALID;
        }
        $dryRun = $input->getOption('dry-run') === true;

        $io->title(sprintf('Snapshot prune-orphans — snapshots whose stream is gone%s', $dryRun ? ' (DRY RUN)' : ''));

        if ($dryRun) {
            $io->success(sprintf('Would prune %d orphan snapshot(s). Nothing deleted.', $this->snapshots->countOrphans()));

            return Command::SUCCESS;
        }

        $io->success(sprintf('Pruned %d orphan snapshot(s).', $this->snapshots->pruneOrphans($batch)));

        return Command::SUCCESS;
    }
}
