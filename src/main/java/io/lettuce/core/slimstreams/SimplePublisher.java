package io.lettuce.core.slimstreams;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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

    private final List<SubscriberState<T>> subscribers = new CopyOnWriteArrayList<>();

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
            // Unicast mode: only one subscriber allowed
            if (!subscriberState.compareAndSet(null, state)) {
                subscriber.onError(new IllegalStateException("Only one subscriber allowed"));
                return;
            }
        } else {
            // Multicast mode: support multiple subscribers
            // Copy all currently buffered elements to the new subscriber's buffer
            if (state.subscriberBuffer != null) {
                state.subscriberBuffer.addAll(buffer);
            }
            subscribers.add(state);
            // Also set the main subscriber state for backward compatibility
            subscriberState.set(state);
        }

        subscriber.onSubscribe(state.subscription);

        // Mark that onSubscribe has completed - now drain can proceed
        state.markOnSubscribeCompleted();

        // Deliver any buffered elements or terminal signals
        state.drain();
    }

    /**
     * Emits an element to the subscriber(s).
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

        boolean emitted;

        // In multicast mode, add to shared buffer AND copy to each subscriber's buffer
        if (multicast) {
            emitted = buffer.offer(element); // Keep in shared buffer for new subscribers
            for (SubscriberState<T> state : subscribers) {
                if (state.subscriberBuffer != null) {
                    state.subscriberBuffer.offer(element);
                }
                state.drain();
            }
        } else {
            // In unicast mode, use shared buffer
            emitted = buffer.offer(element);
            SubscriberState<T> state = subscriberState.get();
            if (state != null) {
                state.drain();
            }
        }

        return emitted;
    }

    /**
     * Completes the publisher.
     */
    public void complete() {
        if (completed.compareAndSet(false, true)) {
            // Drain all subscribers in multicast mode
            if (multicast) {
                for (SubscriberState<T> state : subscribers) {
                    state.drain();
                }
            } else {
                // Drain single subscriber in unicast mode
                SubscriberState<T> state = subscriberState.get();
                if (state != null) {
                    state.drain();
                }
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
            // Drain all subscribers in multicast mode
            if (multicast) {
                for (SubscriberState<T> state : subscribers) {
                    state.drain();
                }
            } else {
                // Drain single subscriber in unicast mode
                SubscriberState<T> state = subscriberState.get();
                if (state != null) {
                    state.drain();
                }
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
     * Gets the number of buffered elements. In multicast mode, returns the maximum buffer size across all subscribers.
     *
     * @return the buffer size
     */
    public int getBufferSize() {
        if (multicast) {
            int maxSize = 0;
            for (SubscriberState<T> state : subscribers) {
                if (state.subscriberBuffer != null) {
                    maxSize = Math.max(maxSize, state.subscriberBuffer.size());
                }
            }
            return maxSize;
        } else {
            return buffer.size();
        }
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

        private final AtomicBoolean onSubscribeCompleted = new AtomicBoolean(false);

        // Per-subscriber buffer for multicast mode
        private final Queue<T> subscriberBuffer;

        SubscriberState(Subscriber<? super T> subscriber, SimplePublisher<T> publisher) {
            this.subscriber = subscriber;
            this.publisher = publisher;
            // In multicast mode, each subscriber gets its own buffer
            this.subscriberBuffer = publisher.multicast ? new ConcurrentLinkedQueue<>() : null;
            this.subscription = new SimpleSubscription(subscriber, n -> drain(), () -> {
                // Cancellation handler
                if (publisher.multicast) {
                    // In multicast mode, clear only this subscriber's buffer and remove from list
                    if (subscriberBuffer != null) {
                        subscriberBuffer.clear();
                    }
                    publisher.subscribers.remove(this);
                } else {
                    // In unicast mode, clear shared buffer and drop subscriber reference
                    publisher.buffer.clear();
                    publisher.subscriberState.compareAndSet(this, null);
                }
            });
        }

        void markOnSubscribeCompleted() {
            onSubscribeCompleted.set(true);
        }

        void drain() {
            // Don't drain until onSubscribe has completed
            if (!onSubscribeCompleted.get()) {
                return;
            }

            if (!draining.compareAndSet(false, true)) {
                return;
            }

            // Select the appropriate buffer (per-subscriber for multicast, shared for unicast)
            Queue<T> bufferToUse = publisher.multicast ? subscriberBuffer : publisher.buffer;

            try {
                while (true) {
                    if (subscription.isCancelled()) {
                        if (bufferToUse != null) {
                            bufferToUse.clear();
                        }
                        return;
                    }

                    // Check for error (only signal once)
                    Throwable error = publisher.error.get();
                    if (error != null && terminated.compareAndSet(false, true)) {
                        if (bufferToUse != null) {
                            bufferToUse.clear();
                        }
                        subscriber.onError(error);
                        return;
                    }

                    // Check for completion
                    boolean completed = publisher.completed.get();
                    boolean empty = bufferToUse == null || bufferToUse.isEmpty();

                    if (completed && empty && terminated.compareAndSet(false, true)) {
                        subscriber.onComplete();
                        return;
                    }

                    // Emit elements if there's demand
                    if (subscription.hasDemand() && !empty && bufferToUse != null) {
                        T element = bufferToUse.poll();
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
