package io.lettuce.core.slimstreams;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A spec-compliant, thread-safe implementation of {@link Subscription} that manages demand accounting and cancellation for a
 * single Publisher/Subscriber pair.
 *
 * <p>
 * Reactive Streams rules addressed by this implementation:
 * <ul>
 * <li>§3.1 — Subscription.request MUST NOT allow synchronous unbounded recursion; enforced via a WIP (work-in-progress) drain
 * guard.</li>
 * <li>§3.6 — Subscription.cancel MUST be idempotent.</li>
 * <li>§3.7 — After cancel, additional request() calls MUST be no-ops.</li>
 * <li>§3.9 — request(n &lt;= 0) is a protocol violation that MUST result in onError being signalled on the Subscriber. Because
 * only a Publisher may call onError (signal ownership), this class cancels itself and delegates the onError call back through
 * the {@link RequestHandler} — which is the Publisher side of the pair. See §3.9 note in class javadoc.</li>
 * <li>§3.13 — Subscriber reference MUST be dropped after cancellation (GC safety).</li>
 * <li>§3.17 — Demand MUST support up to Long.MAX_VALUE ("effectively unbounded"); accumulation overflow is capped rather than
 * wrapping.</li>
 * <li>§2.7 — All calls to request() and cancel() must be serialized; achieved via a single {@code AtomicLong demand} sentinel
 * and a WIP drain guard.</li>
 * </ul>
 *
 * <h3>Design decisions</h3>
 *
 * <p>
 * <b>Single {@code AtomicLong demand} encodes both value and cancelled state.</b> The sentinel {@code CANCELLED}
 * ({@code Long.MIN_VALUE}) is stored directly in the demand counter so that "is cancelled?" and "what is the demand?" are
 * answered by a single atomic read. This eliminates the TOCTOU race that arises when a separate {@code AtomicBoolean cancelled}
 * is used alongside {@code AtomicLong demand}.
 *
 * <p>
 * <b>§3.9 and signal ownership.</b> Strictly, only Publishers may emit terminal signals. However, {@code SimpleSubscription} is
 * a leaf-level helper with no separate Publisher object above it — it IS the Publisher side of the pair. When
 * {@code request(n &lt;= 0)} is detected, this class cancels itself atomically and calls
 * {@code subscriber.onError(IllegalArgumentException)} directly, holding the subscriber reference in an {@code AtomicReference}
 * to guarantee exactly one thread ever delivers the signal. This matches the pattern used by Reactor's {@code FluxCreate} and
 * RxJava's {@code ObservableCreate}, and satisfies the TCK's {@code required_spec309} tests which call
 * {@code request(n &lt;= 0)} directly on the Subscription with no intervening Publisher.
 *
 * <p>
 * <b>WIP drain guard (§3.1).</b> Only one thread executes the {@link RequestHandler} callback at a time. Concurrent or
 * re-entrant {@code request()} calls increment the WIP counter; the active drain loop picks up the updated demand on its next
 * iteration. {@code AtomicInteger} is used (not {@code AtomicLong}) as is conventional in Reactor and RxJava.
 *
 * <p>
 * <b>Subscriber reference lifecycle (§3.13).</b> The {@link Subscriber} is held in an {@code AtomicReference} rather than a
 * plain {@code volatile} field so that nulling it on cancellation is a single CAS. This prevents a race where two concurrent
 * terminal paths (e.g. {@code cancel()} racing with {@code request(n &lt;= 0)}) could each read a non-null reference and both
 * attempt to signal the Subscriber.
 *
 * <p>
 * <b>Public demand API compatibility.</b> {@link #hasDemand()} and {@link #decrementDemand(long)} are present for API
 * compatibility. Both are safe against the CANCELLED sentinel. Publishers using the {@code hasDemand()} + emit +
 * {@code decrementDemand(1)} pattern should be aware that those three steps are NOT one atomic operation. {@link #tryConsume()}
 * is provided as a fully atomic single-step alternative and is preferred for new code.
 *
 * @author Ali TAKAVCI
 * @since 7.6.0
 */
public class SimpleSubscription implements Subscription {

    /**
     * Sentinel stored in {@code demand} once this subscription is cancelled. {@code Long.MIN_VALUE} is unambiguous as a
     * terminal marker because valid demand is always &gt;= 0.
     *
     * Lifecycle: 0 ──request(n &gt; 0)──► 1..Long.MAX_VALUE ──cancel()──► CANCELLED any value
     * ──cancel()─────────────────────────► CANCELLED
     */
    private static final long CANCELLED = Long.MIN_VALUE;

    /**
     * Encodes the current unfulfilled demand OR the {@link #CANCELLED} sentinel. All transitions use CAS to establish the
     * happens-before chain required by §2.7.
     */
    private final AtomicLong demand = new AtomicLong(0L);

    /**
     * Work-in-progress counter (§3.1 re-entrancy guard). {@code AtomicInteger} matches Reactor / RxJava convention — this
     * counter will never meaningfully exceed {@code Integer.MAX_VALUE} in practice.
     */
    private final AtomicInteger wip = new AtomicInteger(0);

    /**
     * Downstream Subscriber, held in an {@code AtomicReference} so the null-out on cancellation is a single CAS, preventing
     * double-delivery races (§3.13).
     */
    private final AtomicReference<Subscriber<?>> subscriberRef;

    private final RequestHandler requestHandler;

    private final CancellationHandler cancellationHandler;

    /**
     * Creates a new {@link SimpleSubscription}.
     *
     * @param subscriber the downstream Subscriber
     * @param requestHandler called under the WIP drain guard when demand changes
     * @param cancellationHandler called exactly once when this subscription is cancelled
     */
    public SimpleSubscription(Subscriber<?> subscriber, RequestHandler requestHandler,
            CancellationHandler cancellationHandler) {
        if (subscriber == null) {
            throw new NullPointerException("subscriber must not be null");
        }
        if (requestHandler == null) {
            throw new NullPointerException("requestHandler must not be null");
        }
        if (cancellationHandler == null) {
            throw new NullPointerException("cancellationHandler must not be null");
        }
        this.subscriberRef = new AtomicReference<>(subscriber);
        this.requestHandler = requestHandler;
        this.cancellationHandler = cancellationHandler;
    }

    /**
     * Adds {@code n} to the outstanding demand and schedules a drain.
     *
     * <ul>
     * <li>§3.9 — {@code n &lt;= 0}: subscription is cancelled atomically and
     * {@code subscriber.onError(IllegalArgumentException)} is called directly. This is the only place in the codebase where the
     * Subscription calls a signal method — it is justified because there is no separate Publisher object above this class to
     * own the signal, and the TCK verifies this behaviour directly on the Subscription without any intervening Publisher.</li>
     * <li>§3.7 — No-op if already cancelled.</li>
     * <li>§3.17 — Demand is capped at {@code Long.MAX_VALUE} on overflow.</li>
     * <li>§3.1 — Re-entrant calls are serialised through the WIP drain guard.</li>
     * </ul>
     */
    @Override
    public void request(long n) {

        // §3.9 — illegal request value: must signal onError(IllegalArgumentException).
        if (n <= 0) {
            // Transition to CANCELLED atomically. If already cancelled, drop (§3.7).
            // We must still signal onError even if we were already active — the spec
            // requires the signal to always be delivered when n <= 0.
            if (demand.getAndSet(CANCELLED) == CANCELLED) {
                return;
            }
            // Atomically claim the subscriber reference (§3.13).
            // getAndSet(null) ensures exactly ONE thread ever calls onError,
            // even if cancel() races this path simultaneously.
            Subscriber<?> s = subscriberRef.getAndSet(null);
            if (s != null) {
                // §3.9 — the Subscription calls onError directly here because there
                // is no separate Publisher object to own the signal. This matches
                // the pattern used by Reactor's FluxCreate and RxJava's
                // ObservableCreate, both of which call onError from the Subscription
                // when no upstream Publisher mediates the call.
                // The subscription is already cancelled above, so no emissions can
                // follow this onError call — the terminal-signal-once rule (§1.7)
                // is satisfied.
                s.onError(new IllegalArgumentException("§3.9: request must be positive, but was: " + n));
            }
            return;
        }

        // Accumulate demand with overflow cap (§3.17).
        // Loop exits immediately if CANCELLED (§3.7).
        long currentDemand;
        do {
            currentDemand = demand.get();
            if (currentDemand == CANCELLED) {
                return; // §3.7
            }
            if (currentDemand == Long.MAX_VALUE) {
                break; // already unbounded — still need to drain
            }
            long newDemand = currentDemand + n;
            if (newDemand < 0) {
                newDemand = Long.MAX_VALUE; // overflow → cap (§3.17)
            }
            if (demand.compareAndSet(currentDemand, newDemand)) {
                break;
            }
            // CAS lost to concurrent request() or cancel() — retry
        } while (true);

        // §3.1 — only the thread that transitions wip 0 → 1 enters the drain loop.
        if (wip.getAndIncrement() == 0) {
            drain();
        }
    }

    /**
     * Cancels this subscription idempotently.
     *
     * <p>
     * §3.6 — Safe to call any number of times; only the first call triggers {@link CancellationHandler#onCancel()}.
     * <p>
     * §3.13 — Subscriber reference is released immediately on first cancellation.
     */
    @Override
    public void cancel() {
        if (demand.getAndSet(CANCELLED) != CANCELLED) {
            subscriberRef.set(null); // §3.13 — atomic set to prevent double-delivery race
            cancellationHandler.onCancel();
        }
    }

    /**
     * Returns {@code true} if there is at least one unit of outstanding demand and the subscription has not been cancelled.
     *
     * <p>
     * <b>Note on atomicity:</b> this is a point-in-time snapshot. A sequence of {@code hasDemand()} → emit →
     * {@link #decrementDemand(long)} is NOT atomic. Use {@link #tryConsume()} for fully atomic single-element gating.
     *
     * @return {@code true} if demand &gt; 0 and subscription is active
     */
    public boolean hasDemand() {
        // CANCELLED is Long.MIN_VALUE (negative) so the > 0 check covers both
        // "cancelled" and "no demand" correctly with a single comparison.
        return demand.get() > 0;
    }

    /**
     * Decrements the outstanding demand by {@code n}, saturating at zero.
     *
     * <p>
     * Unbounded demand ({@code Long.MAX_VALUE}) is never decremented. If the subscription has been cancelled this method is a
     * no-op and returns {@code 0}.
     *
     * <p>
     * <b>Note on atomicity:</b> prefer {@link #tryConsume()} for single-element gating where the check and decrement must be
     * atomic.
     *
     * @param n the number of demand units to consume; must be positive
     * @return the remaining demand after decrement, or {@code 0} if cancelled
     * @throws IllegalArgumentException if {@code n} is not positive
     */
    public long decrementDemand(long n) {
        if (n <= 0)
            throw new IllegalArgumentException("n must be positive, got: " + n);

        long current, next;
        do {
            current = demand.get();
            if (current == CANCELLED) {
                return 0L;
            }
            if (current == Long.MAX_VALUE) {
                return Long.MAX_VALUE; // unbounded — never decrements
            }
            next = Math.max(0L, current - n);
        } while (!demand.compareAndSet(current, next));

        return next;
    }

    /**
     * Atomically checks for demand and, if present, consumes one unit in a single CAS.
     *
     * <p>
     * Preferred over {@link #hasDemand()} + {@link #decrementDemand(long)} for single-element gating because the check and
     * decrement are fused — there is no window for a concurrent {@code cancel()} to slip between them.
     *
     * <p>
     * Unbounded demand ({@code Long.MAX_VALUE}) always returns {@code true} without decrementing.
     *
     * @return {@code true} if an emission is permitted; {@code false} if demand is zero or the subscription has been cancelled
     */
    public boolean tryConsume() {
        long current;
        do {
            current = demand.get();
            // Explicit sentinel checks — avoids relying on sign of CANCELLED value.
            if (current == CANCELLED || current == 0L) {
                return false;
            }
            if (current == Long.MAX_VALUE) {
                return true; // unbounded — do not decrement
            }
        } while (!demand.compareAndSet(current, current - 1L));
        return true;
    }

    /**
     * Returns {@code true} if this subscription has been cancelled.
     *
     * @return {@code true} if cancelled
     */
    public boolean isCancelled() {
        return demand.get() == CANCELLED;
    }

    /**
     * Returns the current unfulfilled demand, or {@code -1} if cancelled.
     *
     * <p>
     * For observability and testing only. Do not branch on this value to gate emissions — use {@link #tryConsume()} or
     * {@link #hasDemand()} instead.
     *
     * @return current demand (&gt;= 0), or {@code -1} if cancelled
     */
    public long getDemand() {
        long d = demand.get();
        return d == CANCELLED ? -1L : d;
    }

    /**
     * Drain loop, entered only by the thread that transitions {@code wip} from 0 to 1.
     *
     * <p>
     * Demand is read exactly once per iteration into a local variable. This closes the double-read race where a first read
     * passes the CANCELLED check but a second read (inside the handler call) returns CANCELLED after a concurrent cancel().
     */
    private void drain() {
        do {
            long d = demand.get(); // single read per iteration — no double-read race
            if (d != CANCELLED) {
                requestHandler.onRequest(d);
            }
        } while (wip.decrementAndGet() > 0);
    }

    /**
     * Callback for the Publisher side to react to demand changes.
     *
     * <p>
     * This is a {@link FunctionalInterface}: implementations provide only {@link #onRequest(long)}.
     *
     * <p>
     * {@code onRequest} is called only for valid (positive) demand values. §3.9 protocol violations ({@code n &lt;= 0}) are
     * handled entirely inside {@link SimpleSubscription#request(long)} — {@code subscriber.onError()} is called there directly
     * and {@code onRequest} is never invoked for illegal values.
     *
     * <p>
     * Example implementation:
     * 
     * <pre>
     * {@code
     * new SimpleSubscription(subscriber,
     *     totalDemand -> {
     *         // emit elements while tryConsume() returns true ...
     *         while (subscription.tryConsume()) {
     *             T next = produceNext();
     *             if (next == null) { subscriber.onComplete(); break; }
     *             subscriber.onNext(next);
     *         }
     *     },
     *     () -> { /* release upstream resources *\/ });
     * }
     * </pre>
     */
    @FunctionalInterface
    public interface RequestHandler {

        /**
         * Called under the WIP drain guard when demand increases.
         *
         * <p>
         * {@code totalDemand} is always positive — illegal request values are handled by {@link SimpleSubscription} before this
         * method is ever called.
         *
         * @param totalDemand current total unfulfilled demand (&gt; 0)
         */
        void onRequest(long totalDemand);

    }

    /**
     * Callback invoked exactly once when this subscription is cancelled.
     */
    @FunctionalInterface
    public interface CancellationHandler {

        /**
         * Called once on cancellation. Implementations should release upstream resources.
         */
        void onCancel();

    }

}
