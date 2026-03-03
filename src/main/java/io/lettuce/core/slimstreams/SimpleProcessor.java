package io.lettuce.core.slimstreams;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    private final Queue<R> buffer = new ConcurrentLinkedQueue<>();

    private final AtomicReference<Subscription> upstreamSubscription = new AtomicReference<>();

    private final AtomicReference<SubscriberState<R>> downstreamState = new AtomicReference<>();

    private final AtomicBoolean terminated = new AtomicBoolean(false);

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final AtomicBoolean completed = new AtomicBoolean(false);

    private final Transformer<T, R> transformer;

    private final boolean multicast;

    /**
     * Creates a new unicast {@link SimpleProcessor} with an identity transformer.
     */
    @SuppressWarnings("unchecked")
    public SimpleProcessor() {
        this((Transformer<T, R>) (Transformer<T, T>) element -> element, false);
    }

    /**
     * Creates a new unicast {@link SimpleProcessor} with the specified transformer.
     *
     * @param transformer the transformer to apply to elements
     */
    public SimpleProcessor(Transformer<T, R> transformer) {
        this(transformer, false);
    }

    /**
     * Creates a new {@link SimpleProcessor} with the specified transformer and multicast mode.
     *
     * @param transformer the transformer to apply to elements
     * @param multicast {@code true} to allow multiple downstream subscribers, {@code false} for unicast
     */
    public SimpleProcessor(Transformer<T, R> transformer, boolean multicast) {
        if (transformer == null) {
            throw new NullPointerException("transformer must not be null");
        }
        this.transformer = transformer;
        this.multicast = multicast;
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

        // Request initial demand from upstream
        s.request(1);
    }

    @Override
    public void onNext(T t) {
        if (t == null) {
            throw new NullPointerException("Element must not be null");
        }

        if (terminated.get()) {
            return;
        }

        try {
            R transformed = transformer.transform(t);
            if (transformed != null) {
                buffer.offer(transformed);

                SubscriberState<R> state = downstreamState.get();
                if (state != null) {
                    state.drain();
                }
            }

            // Request more from upstream if downstream has demand
            Subscription upstream = upstreamSubscription.get();
            if (upstream != null) {
                SubscriberState<R> state = downstreamState.get();
                if (state == null || state.subscription.hasDemand()) {
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

        if (!terminated.compareAndSet(false, true)) {
            return;
        }

        error.set(t);

        SubscriberState<R> state = downstreamState.get();
        if (state != null) {
            state.drain();
        }
    }

    @Override
    public void onComplete() {
        if (!terminated.compareAndSet(false, true)) {
            return;
        }

        completed.set(true);

        SubscriberState<R> state = downstreamState.get();
        if (state != null) {
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

        if (!multicast) {
            if (!downstreamState.compareAndSet(null, state)) {
                subscriber.onError(new IllegalStateException("Only one subscriber allowed"));
                return;
            }
        } else {
            downstreamState.set(state);
        }

        subscriber.onSubscribe(state.subscription);

        // Deliver any buffered elements or terminal signals
        state.drain();
    }

    /**
     * Checks if the processor has been terminated.
     *
     * @return {@code true} if terminated, {@code false} otherwise
     */
    public boolean isTerminated() {
        return terminated.get();
    }

    /**
     * Checks if the processor has completed successfully.
     *
     * @return {@code true} if completed, {@code false} otherwise
     */
    public boolean isCompleted() {
        return completed.get();
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
     * Gets the number of buffered elements.
     *
     * @return the buffer size
     */
    public int getBufferSize() {
        return buffer.size();
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

        SubscriberState(Subscriber<? super R> subscriber, SimpleProcessor<?, R> processor) {
            this.subscriber = subscriber;
            this.processor = processor;
            this.subscription = new SimpleSubscription(subscriber, n -> {
                drain();
                // Request more from upstream when downstream requests
                Subscription upstream = processor.upstreamSubscription.get();
                if (upstream != null && !processor.terminated.get()) {
                    upstream.request(n);
                }
            }, () -> {
                // Cancellation handler - clear buffer and drop subscriber reference
                processor.buffer.clear();
                processor.downstreamState.compareAndSet(this, null);
                Subscription upstream = processor.upstreamSubscription.get();
                if (upstream != null) {
                    upstream.cancel();
                }
            });
        }

        void drain() {
            if (!draining.compareAndSet(false, true)) {
                return;
            }

            try {
                while (true) {
                    if (subscription.isCancelled()) {
                        processor.buffer.clear();
                        return;
                    }

                    // Check for error (only signal once)
                    Throwable error = processor.error.get();
                    if (error != null && terminated.compareAndSet(false, true)) {
                        processor.buffer.clear();
                        subscriber.onError(error);
                        return;
                    }

                    // Check for completion
                    boolean completed = processor.completed.get();
                    boolean empty = processor.buffer.isEmpty();

                    if (completed && empty && terminated.compareAndSet(false, true)) {
                        subscriber.onComplete();
                        return;
                    }

                    // Emit elements if there's demand
                    if (subscription.hasDemand() && !empty) {
                        R element = processor.buffer.poll();
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
            if (!terminated.get() && subscription.hasDemand() && !processor.buffer.isEmpty() && !subscription.isCancelled()) {
                drain();
            }
        }

    }

}
