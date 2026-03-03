package io.lettuce.core.slimstreams;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A simple, thread-safe implementation of {@link Subscription} that manages demand and cancellation.
 * <p>
 * This implementation follows the Reactive Streams specification:
 * <ul>
 * <li>Supports unbounded demand (Long.MAX_VALUE)</li>
 * <li>Accumulates demand safely without overflow</li>
 * <li>Ensures thread-safe cancellation</li>
 * <li>Prevents negative or zero requests</li>
 * </ul>
 *
 * @author Lettuce Contributors
 * @since 7.6.0
 */
public class SimpleSubscription implements Subscription {

    private final AtomicLong demand = new AtomicLong(0);

    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    private final Subscriber<?> subscriber;

    private final RequestHandler requestHandler;

    private final CancellationHandler cancellationHandler;

    /**
     * Creates a new {@link SimpleSubscription}.
     *
     * @param subscriber the subscriber to signal errors to
     * @param requestHandler the handler to be called when demand is requested
     * @param cancellationHandler the handler to be called when subscription is cancelled
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
        this.subscriber = subscriber;
        this.requestHandler = requestHandler;
        this.cancellationHandler = cancellationHandler;
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
            cancel();
            subscriber.onError(new IllegalArgumentException("§3.9: non-positive request signals are illegal"));
            return;
        }

        if (cancelled.get()) {
            return;
        }

        // Add demand, handling overflow by capping at Long.MAX_VALUE
        long currentDemand, newDemand;
        do {
            currentDemand = demand.get();
            if (currentDemand == Long.MAX_VALUE) {
                // Already unbounded
                return;
            }
            newDemand = currentDemand + n;
            // Check for overflow
            if (newDemand < 0) {
                newDemand = Long.MAX_VALUE;
            }
        } while (!demand.compareAndSet(currentDemand, newDemand));

        requestHandler.onRequest(n);
    }

    @Override
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            cancellationHandler.onCancel();
        }
    }

    /**
     * Checks if this subscription has been cancelled.
     *
     * @return {@code true} if cancelled, {@code false} otherwise
     */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Gets the current demand.
     *
     * @return the current demand
     */
    public long getDemand() {
        return demand.get();
    }

    /**
     * Decrements the demand by the specified amount.
     *
     * @param n the amount to decrement
     * @return the remaining demand after decrement
     */
    public long decrementDemand(long n) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must be positive");
        }

        long currentDemand, newDemand;
        do {
            currentDemand = demand.get();
            if (currentDemand == Long.MAX_VALUE) {
                // Unbounded demand never decreases
                return Long.MAX_VALUE;
            }
            newDemand = Math.max(0, currentDemand - n);
        } while (!demand.compareAndSet(currentDemand, newDemand));

        return newDemand;
    }

    /**
     * Checks if there is demand available.
     *
     * @return {@code true} if demand is greater than 0, {@code false} otherwise
     */
    public boolean hasDemand() {
        return demand.get() > 0;
    }

    /**
     * Handler for request events.
     */
    @FunctionalInterface
    public interface RequestHandler {

        /**
         * Called when demand is requested.
         *
         * @param n the amount of demand requested
         */
        void onRequest(long n);

        /**
         * Called when an error occurs during request processing.
         *
         * @param t the error
         */
        default void onError(Throwable t) {
            // Default implementation does nothing
        }

    }

    /**
     * Handler for cancellation events.
     */
    @FunctionalInterface
    public interface CancellationHandler {

        /**
         * Called when the subscription is cancelled.
         */
        void onCancel();

    }

}
