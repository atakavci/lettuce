package io.lettuce.core.slimstreams;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.lettuce.core.slimstreams.SimpleSubscription.CancellationHandler;
import io.lettuce.core.slimstreams.SimpleSubscription.RequestHandler;

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
 * @author Ali TAKAVCI
 * @since 7.6.0
 */
public class SimplePublisher<T> implements Publisher<T>, EmissionSink<T> {

    private final List<SubscriberState<T>> subscribers = new CopyOnWriteArrayList<>();

    private volatile boolean completed = false;

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final EmissionController<T> emissionController;

    /**
     * Creates a new {@link SimplePublisher} without emission control.
     */
    public SimplePublisher() {
        this(null);
    }

    /**
     * Creates a new {@link SimplePublisher} with emission control.
     *
     * @param emissionController the controller to handle demand, or null for no control
     */
    public SimplePublisher(EmissionController<T> emissionController) {
        this.emissionController = emissionController;
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("Subscriber must not be null");
        }

        SubscriberState<T> state = createSubscriberState(subscriber);

        subscribers.add(state);

        subscriber.onSubscribe(state.subscription);

        // Mark that onSubscribe has completed - now drain can proceed
        state.markOnSubscribeCompleted();

        // TODO: check this below!!
        // Trigger drain in case completion/error was signaled during onSubscribe
        state.drain(0);

    }

    /**
     * Emits an element to the subscriber(s).
     *
     * @param element the element to emit
     * @return {@code true} if the element was accepted, {@code false} if the publisher is terminated
     */
    @Override
    public boolean emit(T element) {
        if (element == null) {
            throw new NullPointerException("Element must not be null");
        }

        if (completed || error.get() != null) {
            return false;
        }

        // Here its possible that different subscribers might see
        // different order of elements relative to each other.
        // This is something accepted in our design, in favor of avoiding
        // locks/synchronization, but could turn into a
        // fundamental
        // issue depending on the use case.
        for (SubscriberState<T> state : subscribers) {
            state.offer(element);
        }

        // If these two loops are merged, in theory we could cause delays with offer
        // just because
        // drain takes too long for an other subscriber. Having said that, aim here is
        // to benefit
        // from the fact that subscribers can affect the drain on their own.
        // Simple terms; a slow subscriber's drain could block the offer path for
        // everyone:
        for (SubscriberState<T> state : subscribers) {
            state.drain(1);
        }
        return true;
    }

    @Override
    public String toString() {
        return "SimplePublisher{" + "subscribers=" + subscribers + ", completed=" + completed + ", error=" + error + '}';
    }

    /**
     * Completes the publisher.
     * <p>
     * This method is part of the {@link EmissionSink} interface and can be called by emission controllers to signal completion.
     */
    @Override
    public void complete() {
        completed = true;

        // TODO: check this below!!
        // Drain all subscribers to deliver completion signal
        for (SubscriberState<T> state : subscribers) {
            state.drain(1);
        }
    }

    /**
     * Terminates the publisher with an error.
     * <p>
     * This method is part of the {@link EmissionSink} interface and can be called by emission controllers to signal an error.
     *
     * @param t the error
     */
    @Override
    public void error(Throwable t) {
        if (t == null) {
            throw new NullPointerException("Error must not be null");
        }

        if (error.compareAndSet(null, t)) {
            // Drain all subscribers in multicast mode
            for (SubscriberState<T> state : subscribers) {
                state.drain(1);
            }
        }
    }

    /**
     * Checks if the publisher has been completed.
     *
     * @return {@code true} if completed, {@code false} otherwise
     */
    public boolean isCompleted() {
        return completed;
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
     * Gets the number of buffered elements.It returns the maximum buffer size across all subscribers.
     *
     * @return the buffer size
     */
    public int getBufferSize() {
        int maxSize = 0;
        for (SubscriberState<T> state : subscribers) {
            maxSize = Math.max(maxSize, state.subscriberBuffer.size());
        }
        return maxSize;
    }

    protected SubscriberState<T> createSubscriberState(Subscriber<? super T> subscriber) {
        return new SubscriberState<>(subscriber, this);
    }

    /**
     * Internal state for managing a subscriber.
     *
     * @param <T> the type of elements
     */
    protected static class SubscriberState<T> {

        protected final Subscriber<? super T> subscriber;

        protected final SimplePublisher<T> publisher;

        protected final SimpleSubscription subscription;

        protected final AtomicInteger wip = new AtomicInteger(0);

        protected final AtomicBoolean terminated = new AtomicBoolean(false);

        protected volatile boolean onSubscribeCompleted = false;

        // Per-subscriber buffer to support proper multicast
        protected final Queue<T> subscriberBuffer;

        protected SubscriberState(Subscriber<? super T> subscriber, SimplePublisher<T> publisher) {
            this(subscriber, publisher, new ConcurrentLinkedQueue<>());
        }

        protected SubscriberState(Subscriber<? super T> subscriber, SimplePublisher<T> publisher, Queue<T> subscriberBuffer) {
            this.subscriber = subscriber;
            this.publisher = publisher;
            // In multicast mode, each subscriber gets its own buffer
            this.subscriberBuffer = subscriberBuffer;
            // If there's an emission controller, let it handle the demand
            RequestHandler requestHandler = publisher.emissionController != null ? n -> onDemandReceived(n) : n -> drain(n);

            CancellationHandler cancellationHandler = () -> {
                // Cancellation handler
                publisher.subscribers.remove(this);
                // Clear this subscriber's buffer and remove from list
                subscriberBuffer.clear();
            };
            this.subscription = new SimpleSubscription(subscriber, requestHandler, cancellationHandler);
        }

        void onDemandReceived(long requestedAmount) {
            // Ask controller how much it's willing to accept
            // long acceptedAmount =
            publisher.emissionController.onDemand(requestedAmount, publisher);

            // // If controller accepted some demand, update subscription demand
            // if (acceptedAmount > 0 && acceptedAmount < requestedAmount) {
            // // Controller accepted less than requested, adjust demand
            // long toRemove = requestedAmount - acceptedAmount;
            // subscription.decrementDemand(toRemove);
            // }
            drain(requestedAmount);
        }

        void markOnSubscribeCompleted() {
            onSubscribeCompleted = true;
        }

        void offer(T element) {
            if (terminated.get()) {
                return;
            }
            subscriberBuffer.offer(element);
        }

        void drain(long demand) {
            // Don't drain until onSubscribe has completed
            if (!onSubscribeCompleted) {
                return;
            }

            if (wip.getAndIncrement() != 0) {
                return;
            }

            do {
                if (subscription.isCancelled()) {
                    subscriberBuffer.clear();
                    return;
                }

                // Check for completion
                boolean empty = subscriberBuffer.isEmpty();

                // Check for error (only signal once)
                // Signal error only after buffer is empty
                // TODO : not sure around cheking empty flag and trying to comsume all of
                // subscriber buffer instead of clearing it and signalling
                // error right away
                Throwable error = publisher.error.get();
                if (error != null && empty && terminated.compareAndSet(false, true)) {
                    try {
                        subscriber.onError(error);
                    } catch (Throwable ignored) {
                        // Misbehaving Subscriber threw from onError — §2.13 violation.
                        // Nothing further can be done; stream is already terminal.
                    } finally {
                        publisher.subscribers.remove(this); // ← drop reference §3.13
                        subscriberBuffer.clear();
                    }
                    return;
                }

                if (publisher.completed && empty && terminated.compareAndSet(false, true)) {
                    try {
                        subscriber.onComplete();
                    } catch (Throwable ignored) {
                        // Misbehaving Subscriber threw from onComplete — §2.13 violation.
                        // Nothing further can be done; stream is already terminal.
                    } finally {
                        publisher.subscribers.remove(this); // ← drop reference §3.13
                        subscriberBuffer.clear();
                    }
                    return;
                }

                // Emit elements if there's demand
                if (subscription.hasDemand() && !empty) {
                    T element = subscriberBuffer.poll();
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
                    // No demand or no elements: decrement wip and exit only if no
                    // concurrent caller incremented wip while we were in this iteration.
                    // If wip > 0 after decrement a concurrent offer() or request()
                    // arrived — loop again to pick it up instead of exiting.
                    if (wip.decrementAndGet() == 0) {
                        break;
                    }
                }

            } while (true);

        }

        @Override
        public String toString() {
            return "SubscriberState{" + "subscriber=" + subscriber + ", subscription=" + subscription + ", wip=" + wip
                    + ", terminated=" + terminated + ", onSubscribeCompleted=" + onSubscribeCompleted + ", subscriberBuffer="
                    + subscriberBuffer + '}';
        }

    }

}
