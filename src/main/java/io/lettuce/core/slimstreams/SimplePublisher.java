package io.lettuce.core.slimstreams;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A simple, thread-safe implementation of {@link Publisher} that supports buffering and backpressure.
 * <p>
 * This implementation follows the Reactive Streams specification:
 * <ul>
 * <li>Supports multiple subscribers (unicast mode by default)</li>
 * <li>Respects backpressure through demand management</li>
 * <li>Thread-safe emission and subscription</li>
 * <li>Buffers elements when there's no demand</li>
 * </ul>
 *
 * @param <T> the type of elements published
 * @author Lettuce Contributors
 * @since 7.6.0
 */
public class SimplePublisher<T> implements Publisher<T> {

    private final Queue<T> buffer = new ConcurrentLinkedQueue<>();

    private final AtomicReference<SubscriberState<T>> subscriberState = new AtomicReference<>();

    private final AtomicBoolean completed = new AtomicBoolean(false);

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final boolean multicast;

    /**
     * Creates a new unicast {@link SimplePublisher}.
     */
    public SimplePublisher() {
        this(false);
    }

    /**
     * Creates a new {@link SimplePublisher}.
     *
     * @param multicast {@code true} to allow multiple subscribers, {@code false} for unicast
     */
    public SimplePublisher(boolean multicast) {
        this.multicast = multicast;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("Subscriber must not be null");
        }

        SubscriberState<T> state = new SubscriberState<>(subscriber, this);

        if (!multicast) {
            if (!subscriberState.compareAndSet(null, state)) {
                subscriber.onError(new IllegalStateException("Only one subscriber allowed"));
                return;
            }
        } else {
            subscriberState.set(state);
        }

        subscriber.onSubscribe(state.subscription);

        // Deliver any buffered elements or terminal signals
        state.drain();
    }

    /**
     * Emits an element to the subscriber.
     *
     * @param element the element to emit
     * @return {@code true} if the element was accepted, {@code false} if the publisher is terminated
     */
    public boolean emit(T element) {
        if (element == null) {
            throw new NullPointerException("Element must not be null");
        }

        if (completed.get() || error.get() != null) {
            return false;
        }

        buffer.offer(element);

        SubscriberState<T> state = subscriberState.get();
        if (state != null) {
            state.drain();
        }

        return true;
    }

    /**
     * Completes the publisher.
     */
    public void complete() {
        if (completed.compareAndSet(false, true)) {
            SubscriberState<T> state = subscriberState.get();
            if (state != null) {
                state.drain();
            }
        }
    }

    /**
     * Terminates the publisher with an error.
     *
     * @param t the error
     */
    public void error(Throwable t) {
        if (t == null) {
            throw new NullPointerException("Error must not be null");
        }

        if (error.compareAndSet(null, t)) {
            SubscriberState<T> state = subscriberState.get();
            if (state != null) {
                state.drain();
            }
        }
    }

    /**
     * Checks if the publisher has been completed.
     *
     * @return {@code true} if completed, {@code false} otherwise
     */
    public boolean isCompleted() {
        return completed.get();
    }

    /**
     * Checks if the publisher has terminated with an error.
     *
     * @return {@code true} if terminated with error, {@code false} otherwise
     */
    public boolean hasError() {
        return error.get() != null;
    }

    /**
     * Gets the number of buffered elements.
     *
     * @return the buffer size
     */
    public int getBufferSize() {
        return buffer.size();
    }

    /**
     * Internal state for managing a subscriber.
     *
     * @param <T> the type of elements
     */
    private static class SubscriberState<T> {

        private final Subscriber<? super T> subscriber;

        private final SimplePublisher<T> publisher;

        private final SimpleSubscription subscription;

        private final AtomicBoolean draining = new AtomicBoolean(false);

        private final AtomicBoolean terminated = new AtomicBoolean(false);

        SubscriberState(Subscriber<? super T> subscriber, SimplePublisher<T> publisher) {
            this.subscriber = subscriber;
            this.publisher = publisher;
            this.subscription = new SimpleSubscription(subscriber, n -> drain(), () -> {
                // Cancellation handler - clear buffer and drop subscriber reference
                publisher.buffer.clear();
                publisher.subscriberState.compareAndSet(this, null);
            });
        }

        void drain() {
            if (!draining.compareAndSet(false, true)) {
                return;
            }

            try {
                while (true) {
                    if (subscription.isCancelled()) {
                        publisher.buffer.clear();
                        return;
                    }

                    // Check for error (only signal once)
                    Throwable error = publisher.error.get();
                    if (error != null && terminated.compareAndSet(false, true)) {
                        publisher.buffer.clear();
                        subscriber.onError(error);
                        return;
                    }

                    // Check for completion
                    boolean completed = publisher.completed.get();
                    boolean empty = publisher.buffer.isEmpty();

                    if (completed && empty && terminated.compareAndSet(false, true)) {
                        subscriber.onComplete();
                        return;
                    }

                    // Emit elements if there's demand
                    if (subscription.hasDemand() && !empty) {
                        T element = publisher.buffer.poll();
                        if (element != null) {
                            subscription.decrementDemand(1);
                            try {
                                subscriber.onNext(element);
                            } catch (Throwable t) {
                                if (terminated.compareAndSet(false, true)) {
                                    subscription.cancel();
                                    subscriber.onError(t);
                                }
                                return;
                            }
                        }
                    } else {
                        // No demand or no elements, exit drain loop
                        break;
                    }
                }
            } finally {
                draining.set(false);
            }

            // Re-check in case new elements arrived while exiting
            // Only re-drain if not terminated and not cancelled
            if (!terminated.get() && subscription.hasDemand() && !publisher.buffer.isEmpty() && !subscription.isCancelled()) {
                drain();
            }
        }

    }

}
