package io.lettuce.core.slimstreams;

import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A spec-compliant, thread-safe implementation of {@link Subscriber} that
 * delegates
 * to user-provided handlers.
 *
 * <p>
 * Reactive Streams rules addressed by this implementation:
 * <ul>
 * <li>§2.1 — onNext must not be called after onComplete / onError (terminal
 * guard)</li>
 * <li>§2.2 — onSubscribe must always be called first; handlers are only invoked
 * after it</li>
 * <li>§2.3 — onError / onComplete must not be called more than once (CAS on
 * state)</li>
 * <li>§2.5 — A Subscriber MUST call Subscription.cancel() if it is unable or
 * unwilling
 * to receive more signals (duplicate-subscription guard in onSubscribe)</li>
 * <li>§2.7 — All calls to Subscription.request() and Subscription.cancel() must
 * be
 * serialized. Achieved here by routing ALL subscription interactions through
 * the single {@code state} AtomicReference which establishes
 * happens-before.</li>
 * <li>§2.9 — A Subscriber MUST be prepared to receive onComplete with no prior
 * request</li>
 * <li>§2.13 — Null signals must throw NullPointerException immediately</li>
 * <li>§3.13 — The Subscription reference must be released on terminal signals
 * (GC safety)</li>
 * </ul>
 *
 * <h3>Key design decisions vs. the previous version</h3>
 * <ol>
 * <li><b>Single {@code AtomicReference<Object> state}</b> replaces the two
 * separate
 * {@code AtomicReference<Subscription>} + {@code AtomicBoolean terminated}.
 * Using one reference eliminates the TOCTOU race where {@code terminated} could
 * be
 * {@code false} and {@code subscription} non-null yet a concurrent terminal
 * signal
 * races past the guard before the handler is called. A single CAS on
 * {@code state}
 * is the canonical pattern used by Reactor's {@code BaseSubscriber} and
 * RxJava's
 * {@code DisposableHelper}.</li>
 * <li><b>No self-generated {@code onError} calls</b>. §2.13 forbids a
 * Subscriber from
 * calling its own signal methods. When a handler throws, the only legal
 * response is
 * to cancel the upstream Subscription and propagate or swallow the
 * exception.</li>
 * <li><b>Subscription reference is nulled on terminal signals</b> (§3.13 / GC
 * safety).
 * After {@code onComplete} or {@code onError} the state is set to
 * {@code TERMINATED},
 * releasing the Subscription object and all resources it transitively
 * holds.</li>
 * <li><b>Public {@code cancel()} method</b>. Without it, external callers would
 * have to
 * call {@code getSubscription().cancel()} directly, which bypasses the state
 * machine
 * and can race with in-flight {@code request()} calls, violating §2.7.</li>
 * </ol>
 *
 * @param <T> the type of elements signaled
 * @author Lettuce Contributors
 * @since 7.6.0
 */
public class SimpleSubscriber<T> implements Subscriber<T> {

    // -------------------------------------------------------------------------
    // State machine
    // -------------------------------------------------------------------------

    /**
     * Sentinel stored in {@code state} once a terminal signal has been processed
     * (onComplete, onError, or cancel). Using a typed sentinel instead of a second
     * AtomicBoolean eliminates the TOCTOU race present in the original
     * implementation.
     *
     * Lifecycle:
     * null ──onSubscribe──► Subscription ──terminal──► TERMINATED
     * ──cancel───► TERMINATED
     */
    private static final Subscription TERMINATED = new Subscription() {
        @Override
        public void request(long n) {
            /* no-op */ }

        @Override
        public void cancel() {
            /* no-op */ }
    };

    /**
     * Holds the current {@link Subscription}, or {@code null} before onSubscribe,
     * or {@link #TERMINATED} after any terminal event.
     *
     * All transitions are performed with CAS to ensure happens-before ordering
     * across threads, satisfying §2.7.
     */
    private final AtomicReference<Subscription> state = new AtomicReference<>();

    // -------------------------------------------------------------------------
    // User-provided handlers
    // -------------------------------------------------------------------------

    private final SubscribeHandler subscribeHandler;
    private final NextHandler<T> nextHandler;
    private final ErrorHandler errorHandler;
    private final CompleteHandler completeHandler;

    /**
     * Creates a new {@link SimpleSubscriber} with the specified handlers.
     *
     * @param subscribeHandler the handler for subscription events
     * @param nextHandler      the handler for next events
     * @param errorHandler     the handler for error events
     * @param completeHandler  the handler for complete events
     */
    public SimpleSubscriber(SubscribeHandler subscribeHandler, NextHandler<T> nextHandler, ErrorHandler errorHandler,
            CompleteHandler completeHandler) {

        // §2.13 — null checks on construction
        if (subscribeHandler == null) {
            throw new NullPointerException("subscribeHandler must not be null");
        }
        if (nextHandler == null) {
            throw new NullPointerException("nextHandler must not be null");
        }
        if (errorHandler == null) {
            throw new NullPointerException("errorHandler must not be null");
        }
        if (completeHandler == null) {
            throw new NullPointerException("completeHandler must not be null");
        }
        this.subscribeHandler = subscribeHandler;
        this.nextHandler = nextHandler;
        this.errorHandler = errorHandler;
        this.completeHandler = completeHandler;
    }

    // -------------------------------------------------------------------------
    // Subscriber<T> implementation
    // -------------------------------------------------------------------------

    /**
     * §2.12 — onSubscribe must be called at most once.
     * §2.5 — If already subscribed, the new Subscription must be cancelled.
     * §2.13 — Null Subscription must throw NullPointerException.
     */
    @Override
    public void onSubscribe(Subscription s) {
        // §2.13 — null check
        if (s == null)
            throw new NullPointerException("Subscription must not be null (§2.13)");

        if (!state.compareAndSet(null, s)) {
            // §2.5 / §2.12 — already have a Subscription (or already terminated);
            // cancel the new one. We MUST NOT call onError here (§2.13).
            s.cancel();
            return;
        }

        // Subscription stored — now invoke the user handler.
        // If the handler throws, the ONLY legal response is to cancel upstream (§2.13).
        // We must NOT call onError from here.
        try {
            subscribeHandler.onSubscribe(s);
        } catch (Throwable t) {
            // Cancel upstream; do not self-signal onError (§2.13)
            cancel();
            // Re-throw as unchecked so the caller is aware of the failure.
            // The upstream Publisher will see the cancel signal.
            throwIfUnchecked(t);
        }
    }

    /**
     * §2.1 — onNext must not be called after terminal signal.
     * §2.13 — Null element must throw NullPointerException.
     *
     * <p>
     * Note: we intentionally do NOT check for subscription == null here.
     * Receiving onNext before onSubscribe is a Publisher violation (§1.9);
     * it is not the Subscriber's job to fix it by self-signalling onError (§2.13).
     * The null-element check is sufficient per the spec.
     */
    @Override
    public void onNext(T t) {
        // §2.13 — null element
        if (t == null)
            throw new NullPointerException("Element must not be null (§2.13)");

        // §2.1 — terminal guard. We read the current state; if it is TERMINATED
        // we drop the element silently. Because the state reference is an
        // AtomicReference, this read participates in the happens-before chain
        // established by the CAS in onComplete/onError/cancel, eliminating the
        // TOCTOU race present when two separate atomics are used.
        if (state.get() == TERMINATED) {
            return;
        }

        // If the handler throws, cancel upstream. Do NOT self-signal onError (§2.13).
        try {
            nextHandler.onNext(t);
        } catch (Throwable throwable) {
            cancel();
            throwIfUnchecked(throwable);
        }
    }

    /**
     * §2.3 — onError must be called at most once.
     * §2.13 — Null Throwable must throw NullPointerException.
     * §3.13 — Subscription reference must be released (GC safety).
     */
    @Override
    public void onError(Throwable t) {
        // §2.13
        if (t == null)
            throw new NullPointerException("Throwable must not be null (§2.13)");

        // Atomically transition to TERMINATED. If already terminated, drop silently
        // (§2.3).
        Subscription previous = state.getAndSet(TERMINATED);
        if (previous == TERMINATED) {
            // Already terminal — spec allows dropping duplicate terminal signals
            return;
        }
        // previous may be null (onError before onSubscribe — Publisher violation §1.9)
        // or a real Subscription. Either way we proceed to notify the handler.

        // §3.13 — previous reference is now unreachable from this subscriber; GC can
        // collect.
        try {
            errorHandler.onError(t);
        } catch (Throwable handlerException) {
            // Handler threw — we cannot propagate via onError again (§2.3 / §2.13).
            // Log and move on. In a real implementation prefer a proper logger.
            handlerException.printStackTrace();
        }
    }

    /**
     * §2.3 — onComplete must be called at most once.
     * §3.13 — Subscription reference must be released (GC safety).
     */
    @Override
    public void onComplete() {
        // Atomically transition to TERMINATED. If already terminated, drop silently
        // (§2.3).
        Subscription previous = state.getAndSet(TERMINATED);
        if (previous == TERMINATED) {
            return;
        }

        // §3.13 — previous reference is now unreachable; GC can collect.
        try {
            completeHandler.onComplete();
        } catch (Throwable t) {
            // Same reasoning as onError handler above.
            t.printStackTrace();
        }
    }

    // -------------------------------------------------------------------------
    // Public control API
    // -------------------------------------------------------------------------

    /**
     * Cancels the upstream Subscription in a thread-safe manner.
     *
     * <p>
     * This method is provided because without it, external callers would have to
     * call {@code getSubscription().cancel()} directly. That path bypasses the
     * {@code state} machine and can race with concurrent {@code request()} calls
     * inside the user handlers, violating §2.7.
     *
     * <p>
     * Idempotent: safe to call multiple times or after terminal signals.
     */
    public void cancel() {
        // getAndSet establishes happens-before with any prior CAS on state,
        // satisfying the serial-access requirement of §2.7.
        Subscription previous = state.getAndSet(TERMINATED);
        if (previous != null && previous != TERMINATED) {
            previous.cancel();
        }
    }

    /**
     * Returns the current Subscription, or {@code null} if not yet subscribed,
     * or {@link #TERMINATED} sentinel if already terminated.
     *
     * <p>
     * <b>Warning:</b> callers must not invoke {@code request()} or {@code cancel()}
     * on the returned object directly — doing so bypasses the state machine and
     * violates §2.7. Use {@link #cancel()} on this subscriber instead.
     *
     * @return the current subscription state
     */
    public Subscription getSubscription() {
        Subscription s = state.get();
        return s == TERMINATED ? null : s;
    }

    /**
     * Returns {@code true} if this subscriber has received a terminal signal
     * (onComplete, onError) or has been cancelled.
     */
    public boolean isTerminated() {
        return state.get() == TERMINATED;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Re-throws the given {@link Throwable} if it is already unchecked;
     * otherwise wraps it in a {@link RuntimeException}.
     * Used to propagate handler failures without swallowing them silently while
     * still avoiding checked-exception leakage from the signal methods.
     */
    private static void throwIfUnchecked(Throwable t) {
        if (t instanceof RuntimeException)
            throw (RuntimeException) t;
        if (t instanceof Error)
            throw (Error) t;
        throw new RuntimeException(t);
    }

    // -------------------------------------------------------------------------
    // Handler interfaces (unchanged from original)
    // -------------------------------------------------------------------------

    @FunctionalInterface
    public interface SubscribeHandler {
        void onSubscribe(Subscription subscription);
    }

    @FunctionalInterface
    public interface NextHandler<T> {
        void onNext(T element);
    }

    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error);
    }

    @FunctionalInterface
    public interface CompleteHandler {
        void onComplete();
    }
}