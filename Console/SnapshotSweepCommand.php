<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Console;

use LogicException;
use Override;
use Storm\AggregateRepository\AggregateRepositoryManager;
use Storm\AggregateRepository\Exception\UnknownAggregate;
use Storm\AggregateRepository\Snapshot\PersonalDataSnapshotGuard;
use Storm\AggregateRepository\Snapshot\Snapshot;
use Storm\AggregateRepository\Snapshot\SnapshotStore;
use Storm\Clock\PointInTime;
use Storm\Contracts\Aggregate\AggregateIdentity;
use Storm\Contracts\Aggregate\AggregateRepository;
use Storm\Contracts\Aggregate\AggregateRoot;
use Storm\Contracts\Aggregate\SnapshotableAggregateRoot;
use Storm\Contracts\Clock\Clock;
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
 * Produces aggregate snapshots off the writing path, the sweep.
 *
 * For each aggregate configured with a snapshot block and implementing `SnapshotableAggregateRoot`,
 * it discovers the stale streams set-based, via a join of `stream_heads` and `snapshots`, on a
 * composed count/time trigger: `>= threshold` events of drift, optionally throttled by
 * `min_interval_seconds`, OR a changed aggregate whose snapshot is older than `max_age_seconds`; the
 * cold-but-large case the count trigger misses. It reconstitutes each stale stream incrementally
 * through the snapshot-accelerated repository, then upserts a fresh snapshot.
 *
 * Run periodically, by cron or scheduler. Safe to re-run: a freshly snapshotted stream drops out of
 * the stale set; nothing in the write path produces snapshots. `--batch` caps how many are TAKEN
 * per aggregate per run. The discovery paginates keyset-style past failures, so a persistent
 * poison costs a warning and a slot of scan, never the starvation of everything sorted behind it;
 * the next run picks up the rest.
 *
 * Exit code is honest for a scheduler: SUCCESS only when every attempted stream snapshotted;
 * FAILURE when at least one stream failed. Each failure is isolated, warned and skipped, so the run
 * still makes progress, but a cron/K8s/alerting must see it was not clean.
 *
 * Examples:
 *
 * ```bash
 * bin/console storm:snapshot:sweep
 * ```
 *
 * ```bash
 * bin/console storm:snapshot:sweep --batch=200
 * ```
 */
#[AsCommand(name: 'storm:snapshot:sweep', description: 'Take snapshots for aggregates that drifted past their threshold.')]
final class SnapshotSweepCommand extends Command
{
    /**
     * The work ceiling multiplier: a run examines at most this many `--batch` budgets of streams,
     * successes and skips alike. High enough that a poison cluster cannot starve the healthy
     * streams sorted behind it within a run, low enough that a rotten category costs a bounded
     * scan per tick instead of re-scanning itself whole.
     */
    private const int WORK_FACTOR = 10;

    /**
     * @param  array<class-string, array{id: class-string<AggregateIdentity>, category: string, snapshot?: array{threshold: int, max_age_seconds?: int|null, min_interval_seconds?: int|null}}>  $aggregates
     * @param  Clock<PointInTime>  $clock
     */
    public function __construct(
        #[Autowire('%storm.aggregates%')]
        private readonly array $aggregates,
        private readonly AggregateRepositoryManager $manager,
        private readonly SnapshotStore $snapshots,
        private readonly Clock $clock,
        /**
         * The crypto-shredding exclusion: a stream folding a `#[Personal]` event refuses its
         * snapshot loud; persisted state would hold decrypted personal values no forget can reach.
         * Null in a manual wiring skips the check, exactly like an unmarked app's empty map.
         */
        private readonly ?PersonalDataSnapshotGuard $guard = null,
    ) {
        parent::__construct();
    }

    #[Override]
    protected function configure(): void
    {
        $this->addOption('batch', null, InputOption::VALUE_REQUIRED, 'Max snapshots taken per aggregate per run', '1000');
    }

    /**
     * {@inheritDoc}
     *
     * A per-stream failure, an invalid qualifier or one aggregate's read, is contained by the sweep
     * loop, warned and skipped, never propagated; only the failures below abort the run. Contained
     * is not silent: any skipped stream turns the exit code to FAILURE, so a scheduler sees it. The
     * warning names the nature, `BUG` for an SPL LogicException and `Skipped` for a runtime failure,
     * so a dirty run reads as a verdict rather than a list to triage by hand.
     *
     * @throws UnknownAggregate when a configured class is not registered
     * @throws Throwable on a storage failure
     */
    #[Override]
    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        $batch = PositiveIntOption::parse($input->getOption('batch'));
        if ($batch === null) {
            $io->error('--batch must be a positive integer (max snapshots taken per aggregate per run), e.g. --batch=200.');

            return Command::INVALID;
        }

        $total = 0;
        $failed = 0;

        foreach ($this->aggregates as $class => $config) {
            // The `snapshot` block is the sweep opt-in; the interface guards reconstitution.
            if (! isset($config['snapshot']) || ! is_subclass_of($class, SnapshotableAggregateRoot::class)) {
                continue;
            }

            $repository = $this->manager->for($class);
            $snapshot = $config['snapshot'];

            [$taken, $skipped] = $this->sweep(
                $io,
                $config['category'],
                $config['id'],
                $repository, // @phpstan-ignore argument.type (for() narrows to a Snapshotable repo; invariant vs. the wide param, runtime-safe)
                $class,
                (int) $snapshot['threshold'],
                isset($snapshot['max_age_seconds']) ? (int) $snapshot['max_age_seconds'] : null,
                isset($snapshot['min_interval_seconds']) ? (int) $snapshot['min_interval_seconds'] : null,
                $batch,
            );

            if ($taken > 0 || $skipped > 0) {
                $io->writeln(sprintf('  %s: %d snapshot(s), %d skipped', $class, $taken, $skipped));
            }

            $total += $taken;
            $failed += $skipped;
        }

        if ($failed > 0) {
            $io->warning(sprintf('Sweep finished DIRTY: %d snapshot(s) taken, %d stream(s) failed and were skipped — see the warnings above.', $total, $failed));

            return Command::FAILURE;
        }

        $io->success(sprintf('Sweep complete: %d snapshot(s) taken.', $total));

        return Command::SUCCESS;
    }

    /**
     * One aggregate's stale set, snapshotted stream by stream, fault-isolated: a stream that fails,
     * a qualifier that is not a valid identity or a read failure of that one aggregate, is reported and
     * SKIPPED, never allowed to abort the run. Isolation alone is not enough across runs: the stale
     * set is `ORDER BY stream`, deterministic, so a PERSISTENT poison would re-occupy the same sorted
     * slot of a plain LIMIT every run and starve everything behind it once the poisons fill the batch.
     * The loop therefore paginates keyset-style, each page picking up strictly AFTER the last stream
     * seen, poisons included: a poison costs one warning and one slot of scan per run, never the
     * streams behind it. `--batch` budgets SUCCESSES, and a separate work ceiling of `WORK_FACTOR`
     * budgets caps streams EXAMINED whatever their outcome; the cursor does not survive the run, so
     * a work budget alone would hand the starvation back, the front poison eating every run's
     * budget, while a success budget alone would let a failing category re-scan itself whole on
     * every tick. Within the ceiling, progress past a poison cluster is guaranteed; past it, the
     * FAILURE exit is the signal that the category needs a human before it needs a snapshot. A
     * systemic outage still shows: every stream reports, and the exit code turns FAILURE.
     *
     * The crypto-shredding refusal SHORT-CIRCUITS the aggregate for the run: it is a configuration
     * contradiction, the `snapshot` block of a class folding `#[Personal]` events, so the first
     * refused stream proves the config wrong for the class and one warning names it; re-probing and
     * re-warning per stream would repeat the same verdict once per row, which is how a good message
     * becomes an unreadable wall.
     *
     * @param  class-string<AggregateIdentity>  $idClass
     * @param  AggregateRepository<AggregateIdentity, AggregateRoot<AggregateIdentity>>  $repository
     * @param  class-string  $class
     * @return array{int, int} snapshots taken, streams skipped on failure
     *
     * @throws Throwable on a storage failure of the stale-streams read
     */
    private function sweep(SymfonyStyle $io, string $category, string $idClass, AggregateRepository $repository, string $class, int $threshold, ?int $maxAge, ?int $minInterval, int $batch): array
    {
        $taken = 0;
        $skipped = 0;
        $examined = 0;
        $ceiling = $batch * self::WORK_FACTOR;
        $after = null;

        while ($taken < $batch && $examined < $ceiling) {
            $page = $this->snapshots->staleStreams($category, $threshold, $maxAge, $minInterval, $batch, $after);

            if ($page === []) {
                break; // the stale set is exhausted, poisons included, each already warned
            }

            foreach ($page as $stream) {
                $after = $stream; // the keyset cursor advances over successes AND poisons alike

                if ($taken >= $batch || $examined >= $ceiling) {
                    break;
                }

                $examined++;

                try {
                    // the crypto-shredding exclusion, BEFORE the replay is paid: configured-anyway
                    // is refused loud, counted as a skip so the run exits FAILURE, and it ends the
                    // aggregate's sweep for this run, one verdict for the class instead of one
                    // warning per stream
                    $offense = $this->guard?->refusal($stream);
                    if ($offense !== null) {
                        $skipped++;
                        $io->warning(sprintf(
                            'REFUSED %s, and the rest of its category this run: it folds #[Personal] event type [%s] — a snapshot would persist decrypted personal state that no forget can reach. Remove the `snapshot` block for %s; the price of PII in state is full replay (snapshot encryption is the designed v2).',
                            $stream,
                            $offense,
                            $class,
                        ));

                        return [$taken, $skipped];
                    }

                    $id = $idClass::fromString((string) new StreamName($stream)->qualifier);
                    $aggregate = $repository->retrieve($id);

                    if (! $aggregate instanceof SnapshotableAggregateRoot) {
                        continue; // gone / not snapshotable, nothing to cache
                    }

                    $this->snapshots->save(new Snapshot($stream, $class, $aggregate->version(), $aggregate->toSnapshot(), $this->clock->now()));
                    $taken++;
                } catch (Throwable $e) {
                    // the catch stays broad on purpose, that is what keeps one poison from starving the
                    // streams behind it, but the two natures are not the same news: an SPL LogicException
                    // (InvalidArgumentException included) means a malformed stream or a wiring error, a bug
                    // to fix, while anything else is a runtime failure worth a retry. Labelling costs
                    // nothing and turns a wall of warnings into a verdict
                    $skipped++;
                    $io->warning(sprintf(
                        '%s %s: %s',
                        $e instanceof LogicException ? 'BUG' : 'Skipped',
                        $stream,
                        $e->getMessage(),
                    ));
                }
            }
        }

        return [$taken, $skipped];
    }
}
