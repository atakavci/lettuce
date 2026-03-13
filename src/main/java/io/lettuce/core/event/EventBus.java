package io.lettuce.core.event;

import org.reactivestreams.Subscription;

import io.lettuce.core.slimstreams.SimpleSubscriber.NextHandler;

/**
 * Interface for an EventBus. Events can be published over the bus that are delivered to the subscribers.
 *
 * @author Mark Paluch
 * @since 3.4
 */
public interface EventBus {

    /**
     * Subscribe to the event bus and {@link Event}s. The {@link Flux} drops events on backpressure to avoid contention.
     *
     * @return the observable to obtain events.
     */
    <T extends Event> Subscription subscribe(Class<T> eventType, NextHandler<T> subscriber);

    /**
     * Publish a {@link Event} to the bus.
     *
     * @param event the event to publish
     */
    void publish(Event event);

}
