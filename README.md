# Storm Aggregate Repository

The glue between event-sourced aggregates and the event store: persist released events under
optimistic concurrency, reconstitute roots by replay, and accelerate reads with snapshots that can
never lie.

The package sits ABOVE the aggregate domain and the store so that neither depends on the other: it
codes against the Contracts (`AggregateRoot`, `AggregateIdentity`, `SnapshotableAggregateRoot`) and
against Chronicler's ports — any implementation of the aggregate contracts works; the official
`storm-aggregate` traits are a dev dependency, used only by the tests.

## When you reach for it

Every command handler does, twice: `retrieve()` the root, decide, `store()` it. Beyond that pair —

- **opting an aggregate into snapshots**: implement `SnapshotableAggregateRoot` and the manager
  wraps its repository transparently; schedule `storm:snapshot:sweep` to produce them;
- **losing the CAS**: `store()` surfaces Chronicler's `StaleVersion` / `DuplicateVersion` — your
  retry boundary (or the Story bus's) decides what a lost race means;
- **erasing**: not here — `StreamEraser` (Chronicler) is decorated by this package so the snapshot
  dies first; a stream a sweep currently holds refuses the erase with `SnapshotStreamBusy`, before
  anything is deleted, and your caller retries.

## Usage

A repository is bound to one aggregate type + stream category:

```php
use Storm\AggregateRepository\DefaultAggregateRepository;

$repository = new DefaultAggregateRepository(
    aggregateClass: Order::class,
    idClass: OrderId::class,
    category: 'order',
    streamReader: $streamReader,     // Storm\Chronicler\Store\StreamReader (bounded reads)
    decisionAppend: $decisionAppend, // Storm\Chronicler\Store\DecisionAppend (CAS append)
    enricher: $messageEnricher,      // Storm\Message\MessageEnricher (the EnricherRegistry)
);

$order = Order::place(OrderId::generate(), $total);
$repository->store($order);                 // append released events under OCC

$loaded = $repository->retrieve($order->identity());   // ?Order
```

- **`store(AggregateRoot): void`** — the repo's currency is the version (OCC):
  `expectedVersion = version() - count(releasedEvents)`. It wraps each released event into a
  `Message` with the aggregate headers (`AggregateId`/`AggregateIdType`/`AggregateType`/
`AggregateVersion`), runs the
  `MessageEnricher` chain (message id, type, occurred-at, causation), and appends. Storing the
  wrong aggregate type throws `AggregateTypeMismatch`. The global read-your-writes `Position` is
  NOT returned here — that is an outbox / projection concern.
- **`retrieve(AggregateIdentity): ?AggregateRoot`** — replays the stream's records as pure domain
  events into `reconstitute`; `null` when the stream is empty.

## Wiring

In a Symfony app you don't construct one by hand: `AggregateRepositoryManager::for(Order::class)`
builds and caches it from the `storm.aggregates` config (identity class + stream category), and
wraps it in a `SnapshotRepository` when the aggregate implements `SnapshotableAggregateRoot`.

## Snapshots

Snapshot-accelerated reads: `retrieve()` loads the latest snapshot and replays only the events
after it, falling back to a full reconstitution on a miss, a stale shape, or a state bag the
aggregate's own `restoreState` refuses. Snapshots are produced OFF the hot path by
`storm:snapshot:sweep`, never in `store()`, so the write path stays pure.

A snapshot is a cache and can never lie about the authoritative stream:

- **it cannot resurrect**: erasing a stream deletes the snapshot FIRST (the eraser decoration), and
  the erase and the sweep of one stream exclude each other (`SnapshotStreamFence`), so a sweep that
  started before the erase cannot write its replay back after it;
- **it cannot outrun**: on an empty tail, one head probe catches the orphan (stream erased → full
  replay says null, never a phantom) and the recreation (same id living a new, shorter life →
  discard the stale cache of the dead history);
- **it cannot survive its shape**: a version bump of the aggregate's declared snapshot shape
  discards old rows at load, one-time full replay per stream.

**Personal data is excluded from snapshots.** An aggregate whose stream folds a `#[Personal]`
event is refused by the sweep (`PersonalDataSnapshotGuard`): the fold sees decrypted values, so a
snapshot would persist cleartext beside events whose whole protection is being ciphered — and no
forget could reach it. The price of PII in state is full replay; snapshot encryption is a designed
follow-up behind a real trigger.

### Maintenance commands

- **`storm:snapshot:sweep [--batch=N]`** — takes snapshots for streams past their drift/age
  trigger, off the write path. `--batch` (default 1000, strict positive integer — anything else is
  `INVALID`) caps snapshots taken per aggregate per run; discovery paginates keyset-style past
  failures, so a persistent poison stream costs a warning per run, never the streams sorted behind
  it. Exit codes: `0` no stream failed, `1` at least one stream failed (isolated + warned — the run
  still progressed, but your scheduler must see it), `2` invalid option. A stream an erase was
  holding is counted as `deferred`, reported in the summary and left to the next run; it is not a
  failure and does not turn the exit code. Run it periodically; re-running is safe.
- **`storm:snapshot:prune-orphans [--batch=N] [--dry-run]`** — removes snapshot rows whose stream
  no longer exists (structural anti-join, never age-based). Same strict `--batch`.

## The replay guard

Both read paths — full reconstitution and snapshot tail — run through ONE guard,
`AggregateHistoryReplay`: it is the last place a gap, a non-version record or a foreign row can be
told apart before everything downstream sees only domain events and a final version integer. A
violated invariant is `CorruptStreamHistory`, loud.

## Erasing a stream

The bundle decorates Chronicler's `StreamEraser` with `SnapshotDeletingStreamEraser`: erasing a
stream deletes its snapshot FIRST, then the events and head — a surviving snapshot would resurrect
the erased aggregate, or silently pollute a re-created id with the old life's state. The read-side
coherence guard and `prune-orphans` are backstops for crash leftovers and hand-managed stores, not
the mechanism. Projections do not un-project: reset those via the projector, as your own delete
process composes.

Order alone closes the crash window, not the concurrent one: a sweep already replaying that stream
would write the old life back after the erase. Both halves therefore run holding the stream's
`SnapshotStreamFence`, a PostgreSQL transaction-scoped advisory lock the sweep holds too.

**`erase()` can now fail for a reason foreign to the stream.** When a sweep holds the stream, the
erase raises `SnapshotStreamBusy` BEFORE deleting anything, carrying the stream and asking for a
retry; nothing was written, and the same call is safe to issue again. The sweep's hold is bounded,
30 seconds by default, so the retry window is short. A caller driving a deletion process must handle
that refusal, never record the stream as erased on it. The alternative was a silent skip, which
turns a deletion that did not happen into a deletion reported as done.

Two properties come from the lock being transaction-scoped. An erase inside your own transaction
keeps the stream fenced until YOUR commit, so nothing can snapshot it in between. And a failing
erase rolls the snapshot deletion back with it, provided the failure is allowed to propagate: the
decorator never absorbs it, and neither should a caller that wants both halves to settle together.

### Requirements and limits

- PostgreSQL 17 or later. The sweep bounds its own transaction with `transaction_timeout`, which is
  the only Postgres timeout measuring a whole transaction rather than one statement.

- The fence and the sweep must share ONE primary connection, which is what the bundle wires. Two
  advisory locks taken on two connections fence nothing, and a replica cannot participate at all.

- Only the supported path participates. SQL that deletes a stream by hand, or writes a snapshot row
  by hand, takes no lock and is not fenced by anything here.

### Deploying it over an existing cache

The fence prevents new incoherent rows; it says nothing about rows already stored. If the past
integrity of `snapshots` is unknown, the only mechanism that settles it is a full invalidation, and
it has an order:

1. Deploy the fence FIRST. Purging before it reopens the window the purge was meant to close.
2. Drain the sweeps: stop the scheduler and let every in-flight run finish. A sweep still open
   rewrites a row from a replay older than the purge.
3. Then, and only then, invalidate the cache. Snapshots are reconstructible: a removed row costs one
   full replay and loses no data.

Storm ships no automatic purge and no application SQL for this. It is an operator decision on an
operator's data, made once, and its claim is bounded: the race is closed on the supported path and
the stored state was normalized once, not that every snapshot concern is settled.

## Contributor doctrine — the load-bearing rules

**1. Above both, coupled to neither.** The package depends on Contracts and Chronicler's ports;
never on an aggregate implementation. Anything that would make the domain import THIS package, or
this package import a concrete aggregate, is the regression.

**2. The raw driver failure stays BELOW the retry, and is translated at this port.** Both paths
translate into the contracted type, `StorageFailure` with the cause preserved. The rule that matters
is WHERE: the retry decorator sits under this package and pattern-matches the raw driver's transient
failures, so wrapping anywhere beneath it would blind the retry. By the time control returns to
`store()` or `retrieve()` the retry has run and given up, so the port translates without hiding
anything from anyone. The concurrency contracts, `StaleVersion` and `DuplicateVersion`, are not
infrastructure failures and pass through distinct.

**3. Snapshots are cache, events are truth.** Any new snapshot behavior must preserve the three
guarantees above (no resurrection, no outrunning, no surviving its shape) and must never move
snapshot production onto the write path.

**4. One replay guard.** Both read paths share `AggregateHistoryReplay`; fixing an invariant on
one side only, or forking the guard, reintroduces the drift it exists to prevent.

## Tests

```bash
vendor/bin/phpunit src/AggregateRepository/Tests            # unit, from the storm root
vendor/bin/phpunit tests/Integration/AggregateRepository    # real PostgreSQL
```

## Resources

This package is developed in the `chronhub/storm` monorepo; a standalone repository for it is a
READ-ONLY subtree split. Report issues and open pull requests on the monorepo, where the tests,
the architecture gates and the full internal documentation live.

---

*Experimental 0.x: this package changes without deprecation cycles — no backward-compatibility
promise and no legacy layer. Pin an exact 0.x tag or commit for reproducibility; pinning fixes
history, not a stable API. Schema changes are resets, not migrations, and a reset destroys data,
so it stays on disposable environments.*
