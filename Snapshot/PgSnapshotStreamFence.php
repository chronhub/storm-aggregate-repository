<?php

declare(strict_types=1);

namespace Storm\AggregateRepository\Snapshot;

use Closure;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use InvalidArgumentException;
use Storm\AggregateRepository\Exception\SnapshotFenceRefused;
use Symfony\Component\DependencyInjection\Attribute\AsAlias;
use Throwable;

/**
 * PostgreSQL `SnapshotStreamFence` over `pg_try_advisory_xact_lock`.
 *
 * The lock is transaction-scoped by nature, which is the second adapter law for free: it is taken
 * inside the transaction that carries `$work` and released by that transaction's commit or rollback,
 * with nothing to unlock by hand and nothing left held by a process that died. Its key is a single
 * int8 that Postgres derives with `hashtextextended` from a namespace prefix joined to the stream
 * name. The prefix distinguishes this protocol's key inputs from other advisory-lock protocols.
 * Hash collisions can still cause additional contention in the shared int8 lock space.
 *
 * The effective isolation level is read from Postgres in the same round trip as the lock, so a level
 * set by raw SQL or by a pooler, invisible to DBAL's local tracking, costs no extra statement to
 * detect. It is refused, never corrected: this connection is the application's primary and a session
 * default is a wiring fact, not something a fence silently overrides for whatever shares the
 * connection next.
 *
 * Requires PostgreSQL 17 or later, for `transaction_timeout`; the project's floor is 17.
 */
#[AsAlias(SnapshotStreamFence::class)]
final readonly class PgSnapshotStreamFence implements SnapshotStreamFence
{
    /** The key namespace, closed by an ASCII unit separator, a byte no stream name carries. */
    private const string KEY_NAMESPACE = "storm.snapshot.stream\x1f";

    /**
     * The bound measures milliseconds across the entire owned transaction.
     *
     * @throws InvalidArgumentException when the bound is not a positive number of milliseconds
     */
    public function __construct(
        private Connection $connection,
        private int $boundMs = 30_000,
    ) {
        if ($boundMs < 1) {
            throw new InvalidArgumentException(sprintf(
                'The snapshot fence bound must be a positive number of milliseconds, got %d. A zero would '
                .'disable the bound in Postgres, which is the one value this fence must never set.',
                $boundMs,
            ));
        }
    }

    /**
     * {@inheritDoc}
     *
     * `transaction_timeout` is what bounds the hold, and it is the only Postgres timeout that
     * measures the whole transaction: `statement_timeout` would let a replay of many short statements
     * run without limit, and `idle_in_transaction_session_timeout` only watches the gaps between them.
     * It is set `LOCAL`, so it dies with the transaction rather than leaking onto a session the rest
     * of the application shares, and it is set as the first statement of the transaction so it also
     * covers the lock statement and the commit.
     *
     * Reaching the bound TERMINATES the backend, it does not merely cancel a statement. The
     * connection is then dead: no savepoint reaches it, DBAL's own rollback fails against it, and any
     * later statement on the same handle fails too. A failure of this owned transaction is therefore
     * followed by a liveness probe, and a handle that fails it is dropped so the next caller opens a
     * fresh session instead of inheriting a corpse.
     *
     * @throws Exception on a DBAL failure of the fence's own transaction, the expiry of the bound included
     */
    public function tryBounded(string $stream, Closure $work): bool
    {
        if ($this->connection->isTransactionActive()) {
            throw SnapshotFenceRefused::boundedUnderAmbientTransaction($stream);
        }

        try {
            return $this->connection->transactional(
                function (Connection $connection) use ($stream, $work): bool {
                    $connection->executeStatement(sprintf("SET LOCAL transaction_timeout = '%dms'", $this->boundMs));

                    return $this->hold($connection, $stream, $work, ambient: false);
                },
            );
        } catch (Throwable $e) {
            $this->dropIfBroken();

            throw $e;
        }
    }

    /**
     * {@inheritDoc}
     *
     * DBAL nests a transaction as a SAVEPOINT, and the advisory lock is scoped to the TOP-LEVEL
     * transaction rather than to that savepoint, so under an ambient transaction the fence is held
     * until the caller's own commit. That is the erase's guarantee, not a leak: a sweep that could
     * take the stream between the snapshot delete and the caller's commit would put back exactly what
     * the erase removed.
     *
     * The same nesting is what makes a failure inside `$work` restore both halves. The savepoint
     * rollback undoes the snapshot delete along with the erase, and the failure is propagated rather
     * than absorbed, so the caller's transaction cannot commit a snapshot deletion whose stream
     * survived.
     *
     * @throws Exception on a DBAL failure of the transaction the fence joined or opened
     */
    public function tryWithin(string $stream, Closure $work): bool
    {
        $ambient = $this->connection->isTransactionActive();

        return $this->connection->transactional(
            fn (Connection $connection): bool => $this->hold($connection, $stream, $work, $ambient),
        );
    }

    /**
     * Drop the connection when the failure that just crossed took the backend with it.
     *
     * Probed rather than assumed, and one round trip on a path that has already failed is cheap
     * against the alternatives. Closing on every failure would throw away a healthy session, its
     * search path and its session settings included, each time a replay raised an ordinary error;
     * closing on none would hand the next caller a handle that answers every statement with the same
     * dead-connection failure until something else notices.
     */
    private function dropIfBroken(): void
    {
        try {
            $this->connection->executeStatement('SELECT 1');
        } catch (Throwable) {
            $this->connection->close();
        }
    }

    /**
     * Take the stream's lock and the effective isolation level in one statement, then run `$work`
     * only if both answers allow it.
     *
     * @param  Closure():void  $work
     *
     * @throws SnapshotFenceRefused when the effective level is not `READ COMMITTED`
     * @throws Exception on a DBAL failure of the lock statement
     * @throws Throwable propagated from `$work`
     */
    private function hold(Connection $connection, string $stream, Closure $work, bool $ambient): bool
    {
        $row = $connection->fetchNumeric(
            /** @lang PostgreSQL */
            "SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0)), current_setting('transaction_isolation')",
            ['key' => self::KEY_NAMESPACE.$stream],
        );

        $locked = $row !== false && (bool) $row[0];
        $isolation = $row === false ? '' : (string) $row[1];

        // the level is judged BEFORE the lock's answer is acted on: a wrong level is a wiring defect
        // that must surface whether or not this call happened to win the stream, and a refusal that
        // depended on losing a race would be a defect nobody could reproduce
        if ($isolation !== 'read committed') {
            throw $ambient
                ? SnapshotFenceRefused::isolationUnderAmbientTransaction($stream, $isolation)
                : SnapshotFenceRefused::isolationUnderOwnedTransaction($stream, $isolation);
        }

        if (! $locked) {
            return false;
        }

        $work();

        return true;
    }
}
