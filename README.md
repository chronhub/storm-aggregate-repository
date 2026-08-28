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
  dies first.

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

- **it cannot resurrect**: erasing a stream deletes the snapshot FIRST (the eraser decoration);
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
  it. Exit codes: `0` all attempted streams snapshotted, `1` at least one stream failed (isolated +
  warned — the run still progressed, but your scheduler must see it), `2` invalid option. Run it
  periodically; re-running is safe.
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

*Pre-version: this package changes without deprecation cycles — pin a commit if you need
stability, expect resets rather than migrations until the first tagged version.*
