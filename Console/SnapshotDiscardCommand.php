<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Console;

use JsonException;
use Override;
use Storm\AggregateRepository\Snapshot\SnapshotStore;
use Storm\Stream\Exception\InvalidStreamException;
use Storm\Stream\StreamName;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;
use Throwable;

/**
 * Discards one named snapshot, the operator's verb for a snapshot whose state lies at a version the
 * head still holds: the head probe cannot see it, the repository restores it, and a sweep run before
 * the row is gone retakes it at the head with the lie inside. Gone, the stream refolds from its
 * first event on the next load and the sweep retakes a snapshot at the head, the stream's whole
 * height counting as drift.
 *
 * The row is deleted without being loaded first: a load discards a row whose state is unreadable
 * on sight, which would report a snapshot that was there as absent. A stream without a snapshot is
 * reported and is not a failure; a malformed stream name or a bare category is refused before
 * anything is read. Nothing else is touched: the stream, its head and every other snapshot stay.
 * `storm:snapshot:prune-orphans` is the verb for snapshots whose stream is gone.
 *
 * Examples:
 *
 * ```bash
 * bin/console storm:snapshot:discard account-01900000-0000-7000-8000-0000000000de
 * ```
 *
 * ```bash
 * bin/console storm:snapshot:discard account-01900000-0000-7000-8000-0000000000de --json
 * ```
 */
#[AsCommand(name: 'storm:snapshot:discard', description: 'Discard one named snapshot so the stream refolds from its first event and the sweep retakes it.')]
final class SnapshotDiscardCommand extends Command
{
    public function __construct(
        private readonly SnapshotStore $snapshots,
    ) {
        parent::__construct();
    }

    #[Override]
    protected function configure(): void
    {
        $this->addArgument('stream', InputArgument::REQUIRED, 'The stream whose snapshot goes, category-qualifier');
        $this->addOption('json', null, InputOption::VALUE_NONE, 'Print the machine-readable outcome');
    }

    /**
     * {@inheritDoc}
     *
     * @throws Throwable on a storage failure of the delete
     * @throws JsonException when the machine document cannot be encoded
     */
    #[Override]
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        try {
            $stream = new StreamName((string) $input->getArgument('stream'));
        } catch (InvalidStreamException $e) {
            $io->error($e->getMessage());

            return Command::INVALID;
        }
        if ($stream->qualifier === null) {
            $io->error(sprintf('%s names a category, not a stream: a snapshot is discarded one stream at a time.', $stream->toString()));

            return Command::INVALID;
        }

        $discarded = $this->snapshots->delete($stream->toString());
        if ($input->getOption('json') === true) {
            $output->writeln(json_encode(['stream' => $stream->toString(), 'discarded' => $discarded], JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES));
        } elseif ($discarded) {
            $io->success(sprintf('Discarded the snapshot of %s; the stream refolds from its first event on the next load, and the sweep retakes it.', $stream->toString()));
        } else {
            $io->note(sprintf('%s has no snapshot; nothing to discard.', $stream->toString()));
        }

        return Command::SUCCESS;
    }
}
