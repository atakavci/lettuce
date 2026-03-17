package io.lettuce.core.event;

import org.reactivestreams.Subscriber;
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
    void subscribe(Subscriber<Event> subscriber);

    /**
     * Publish a {@link Event} to the bus.
     *
     * @param event the event to publish
     */
    void publish(Event event);

}
