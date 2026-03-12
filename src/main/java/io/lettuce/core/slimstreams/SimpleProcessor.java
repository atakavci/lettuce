package io.lettuce.core.slimstreams;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * A simple, thread-safe implementation of {@link Processor} that acts as both a {@link Subscriber} and a
 * {@link org.reactivestreams.Publisher}.
 * <p>
 * This implementation follows the Reactive Streams specification:
 * <ul>
 * <li>Receives elements from upstream and publishes them downstream</li>
 * <li>Supports multiple subscribers (multicast)</li>
 * <li>Respects backpressure from downstream</li>
 * <li>Propagates terminal signals (complete/error) from upstream to downstream</li>
 * <li>Thread-safe processing and emission</li>
 * <li>Buffers elements when downstream has no demand</li>
 * </ul>
 *
 * @param <T> the type of input elements
 * @param <R> the type of output elements
 * @author Lettuce Contributors
 * @since 7.6.0
 */
public class SimpleProcessor<T, R> implements Processor<T, R> {

    private final List<SubscriberState<R>> subscribers = new CopyOnWriteArrayList<>();

    private final AtomicReference<Subscription> upstreamSubscription = new AtomicReference<>();

    private volatile boolean terminated = false;

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private volatile boolean completed = false;

    private final Transformer<T, R> transformer;

    /**
     * Creates a new unicast {@link SimpleProcessor} with an identity transformer.
     */
    @SuppressWarnings("unchecked")
    public SimpleProcessor() {
        this((Transformer<T, R>) (Transformer<T, T>) element -> element);
    }

    /**
     * Creates a new unicast {@link SimpleProcessor} with the specified transformer.
     *
     * @param transformer the transformer to apply to elements
     */
    public SimpleProcessor(Transformer<T, R> transformer) {
        if (transformer == null) {
            throw new NullPointerException("transformer must not be null");
        }
        this.transformer = transformer;
    }

    // Subscriber implementation (upstream)

    @Override
    public void onSubscribe(Subscription s) {
        if (s == null) {
            throw new NullPointerException("Subscription must not be null");
        }

        if (!upstreamSubscription.compareAndSet(null, s)) {
            // Already subscribed, cancel the new subscription
            s.cancel();
            return;
        }
    }

    @Override
    public void onNext(T t) {
        if (t == null) {
            throw new NullPointerException("Element must not be null");
        }

        if (terminated) {
            return;
        }

        try {
            R transformed = transformer.transform(t);
            if (transformed != null) {
                // Offer to all subscribers (multicast pattern from SimplePublisher)
                for (SubscriberState<R> state : subscribers) {
                    state.offer(transformed);
                }

                // Drain all subscribers (separate loop to avoid blocking offer path)
                for (SubscriberState<R> state : subscribers) {
                    state.drain();
                }
            }

            // Request more from upstream if any downstream subscriber has demand
            // Don't request if there are no subscribers (e.g., all cancelled)
            Subscription upstream = upstreamSubscription.get();
            if (upstream != null && !terminated && !subscribers.isEmpty()) {
                boolean hasDemand = false;
                for (SubscriberState<R> state : subscribers) {
                    if (state.subscription.hasDemand()) {
                        hasDemand = true;
                        break;
                    }
                }
                // Request more if any subscriber has demand
                if (hasDemand) {
                    upstream.request(1);
                }
            }
        } catch (Throwable throwable) {
            onError(throwable);
        }
    }

    @Override
    public void onError(Throwable t) {
        if (t == null) {
            throw new NullPointerException("Error must not be null");
        }

        if (terminated) {
            return;
        }

        terminated = true;
        error.compareAndSet(null, t);

        // Drain all subscribers in multicast mode (pattern from SimplePublisher)
        for (SubscriberState<R> state : subscribers) {
            state.drain();
        }
    }

    @Override
    public void onComplete() {
        if (terminated) {
            return;
        }

        terminated = true;
        completed = true;

        // Drain all subscribers to deliver completion signal (pattern from SimplePublisher)
        for (SubscriberState<R> state : subscribers) {
            state.drain();
        }
    }

    // Publisher implementation (downstream)

    @Override
    public void subscribe(Subscriber<? super R> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("Subscriber must not be null");
        }

        SubscriberState<R> state = new SubscriberState<>(subscriber, this);

        subscribers.add(state);

        subscriber.onSubscribe(state.subscription);

        // Mark that onSubscribe has completed - now drain can proceed
        state.markOnSubscribeCompleted();

        // Deliver any buffered elements or terminal signals
        state.drain();
    }

    /**
     * Checks if the processor has been terminated.
     *
     * @return {@code true} if terminated, {@code false} otherwise
     */
    public boolean isTerminated() {
        return terminated;
    }

    /**
     * Checks if the processor has completed successfully.
     *
     * @return {@code true} if completed, {@code false} otherwise
     */
    public boolean isCompleted() {
        return completed;
    }

    /**
     * Checks if the processor has terminated with an error.
     *
     * @return {@code true} if terminated with error, {@code false} otherwise
     */
    public boolean hasError() {
        return error.get() != null;
    }

    /**
     * Gets the number of buffered elements. Returns the maximum buffer size
     * across all subscribers.
     *
     * @return the buffer size
     */
    public int getBufferSize() {
        int maxSize = 0;
        for (SubscriberState<R> state : subscribers) {
            maxSize = Math.max(maxSize, state.subscriberBuffer.size());
        }
        return maxSize;
    }

    /**
     * Transformer for converting input elements to output elements.
     *
     * @param <T> the input type
     * @param <R> the output type
     */
    @FunctionalInterface
    public interface Transformer<T, R> {

        /**
         * Transforms an input element to an output element.
         *
         * @param element the input element
         * @return the transformed element, or {@code null} to skip
         * @throws Exception if transformation fails
         */
        R transform(T element) throws Exception;

    }

    /**
     * Internal state for managing a downstream subscriber.
     *
     * @param <R> the type of elements
     */
    private static class SubscriberState<R> {

        private final Subscriber<? super R> subscriber;

        private final SimpleProcessor<?, R> processor;

        private final SimpleSubscription subscription;

        private final AtomicBoolean draining = new AtomicBoolean(false);

        private final AtomicBoolean terminated = new AtomicBoolean(false);

        private volatile boolean onSubscribeCompleted = false;

        // Per-subscriber buffer for multicast mode (pattern from SimplePublisher)
        private final Queue<R> subscriberBuffer;

        SubscriberState(Subscriber<? super R> subscriber, SimpleProcessor<?, R> processor) {
            this.subscriber = subscriber;
            this.processor = processor;
            // In multicast mode, each subscriber gets its own buffer
            this.subscriberBuffer = new ConcurrentLinkedQueue<>();
            this.subscription = new SimpleSubscription(subscriber, n -> {
                drain();
                // Request more from upstream when downstream requests
                Subscription upstream = processor.upstreamSubscription.get();
                if (upstream != null && !processor.terminated) {
                    upstream.request(n);
                }
            }, () -> {
                // Cancellation handler - remove subscriber from list and clear its buffer
                processor.subscribers.remove(this);
                subscriberBuffer.clear();
                // Cancel upstream if no more subscribers
                if (processor.subscribers.isEmpty()) {
                    Subscription upstream = processor.upstreamSubscription.get();
                    if (upstream != null) {
                        upstream.cancel();
                    }
                }
            });
        }

        void markOnSubscribeCompleted() {
            onSubscribeCompleted = true;
        }

        void offer(R element) {
            if (terminated.get()) {
                return;
            }
            subscriberBuffer.offer(element);
        }

        void drain() {
            // Don't drain until onSubscribe has completed
            if (!onSubscribeCompleted) {
                return;
            }

            if (!draining.compareAndSet(false, true)) {
                return;
            }

            try {
                while (true) {
                    if (subscription.isCancelled()) {
                        subscriberBuffer.clear();
                        return;
                    }

                    // Check for error first - errors must be propagated immediately
                    Throwable error = processor.error.get();
                    if (error != null && terminated.compareAndSet(false, true)) {
                        subscriberBuffer.clear();
                        subscriber.onError(error);
                        return;
                    }

                    // Check for completion
                    boolean empty = subscriberBuffer.isEmpty();

                    if (processor.completed && empty && terminated.compareAndSet(false, true)) {
                        subscriber.onComplete();
                        return;
                    }

                    // Emit elements if there's demand
                    if (subscription.hasDemand() && !empty) {
                        R element = subscriberBuffer.poll();
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
        }

    }

}
