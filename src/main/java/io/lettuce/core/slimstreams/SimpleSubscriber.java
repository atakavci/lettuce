package io.lettuce.core.slimstreams;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A simple, thread-safe implementation of {@link Subscriber} that delegates to user-provided handlers.
 * <p>
 * This implementation follows the Reactive Streams specification:
 * <ul>
 * <li>Ensures onSubscribe is called at most once</li>
 * <li>Ensures terminal signals (onComplete/onError) are called at most once</li>
 * <li>Prevents signals after terminal state</li>
 * <li>Thread-safe signal handling</li>
 * </ul>
 *
 * @param <T> the type of elements signaled
 * @author Lettuce Contributors
 * @since 7.6.0
 */
public class SimpleSubscriber<T> implements Subscriber<T> {

    private final AtomicReference<Subscription> subscription = new AtomicReference<>();

    private final AtomicBoolean terminated = new AtomicBoolean(false);

    private final SubscribeHandler subscribeHandler;

    private final NextHandler<T> nextHandler;

    private final ErrorHandler errorHandler;

    private final CompleteHandler completeHandler;

    /**
     * Creates a new {@link SimpleSubscriber} with the specified handlers.
     *
     * @param subscribeHandler the handler for subscription events
     * @param nextHandler the handler for next events
     * @param errorHandler the handler for error events
     * @param completeHandler the handler for complete events
     */
    public SimpleSubscriber(SubscribeHandler subscribeHandler, NextHandler<T> nextHandler, ErrorHandler errorHandler,
            CompleteHandler completeHandler) {
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

    @Override
    public void onSubscribe(Subscription s) {
        if (s == null) {
            throw new NullPointerException("Subscription must not be null");
        }

        if (!subscription.compareAndSet(null, s)) {
            // Already subscribed, cancel the new subscription
            s.cancel();
            return;
        }

        try {
            subscribeHandler.onSubscribe(s);
        } catch (Throwable t) {
            onError(t);
        }
    }

    @Override
    public void onNext(T t) {
        if (t == null) {
            onError(new NullPointerException("Element must not be null"));
            return;
        }

        if (terminated.get()) {
            // Already terminated, ignore
            return;
        }

        if (subscription.get() == null) {
            // Not yet subscribed, this is a protocol violation
            onError(new IllegalStateException("onNext called before onSubscribe"));
            return;
        }

        try {
            nextHandler.onNext(t);
        } catch (Throwable throwable) {
            onError(throwable);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (t == null) {
            t = new NullPointerException("Error must not be null");
        }

        if (!terminated.compareAndSet(false, true)) {
            // Already terminated, ignore
            return;
        }

        try {
            errorHandler.onError(t);
        } catch (Throwable throwable) {
            // Error handler threw an exception, log it but don't propagate
            throwable.printStackTrace();
        }
    }

    @Override
    public void onComplete() {
        if (!terminated.compareAndSet(false, true)) {
            // Already terminated, ignore
            return;
        }

        try {
            completeHandler.onComplete();
        } catch (Throwable t) {
            // Complete handler threw an exception, log it but don't propagate
            t.printStackTrace();
        }
    }

    /**
     * Gets the current subscription.
     *
     * @return the subscription, or {@code null} if not yet subscribed
     */
    public Subscription getSubscription() {
        return subscription.get();
    }

    /**
     * Checks if this subscriber has been terminated.
     *
     * @return {@code true} if terminated, {@code false} otherwise
     */
    public boolean isTerminated() {
        return terminated.get();
    }

    /**
     * Handler for subscription events.
     */
    @FunctionalInterface
    public interface SubscribeHandler {

        /**
         * Called when a subscription is received.
         *
         * @param subscription the subscription
         */
        void onSubscribe(Subscription subscription);

    }

    /**
     * Handler for next events.
     *
     * @param <T> the type of elements
     */
    @FunctionalInterface
    public interface NextHandler<T> {

        /**
         * Called when a new element is received.
         *
         * @param element the element
         */
        void onNext(T element);

    }

    /**
     * Handler for error events.
     */
    @FunctionalInterface
    public interface ErrorHandler {

        /**
         * Called when an error occurs.
         *
         * @param error the error
         */
        void onError(Throwable error);

    }

    /**
     * Handler for complete events.
     */
    @FunctionalInterface
    public interface CompleteHandler {

        /**
         * Called when the stream completes.
         */
        void onComplete();

    }

}
