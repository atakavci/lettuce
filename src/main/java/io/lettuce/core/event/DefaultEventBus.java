package io.lettuce.core.event;

import org.reactivestreams.Subscriber;
import io.lettuce.core.event.jfr.EventRecorder;
import io.lettuce.core.slimstreams.SimplePublisher;

/**
 * Default implementation for an {@link EventBus}. Events are published using a {@link Scheduler} and events are recorded
 * through {@link EventRecorder#record(Event) EventRecorder}.
 *
 * @author Mark Paluch
 * @since 3.4
 */
public class DefaultEventBus implements EventBus {

    private final EventRecorder recorder = EventRecorder.getInstance();

    private final SimplePublisher<Event> publisher;

    public DefaultEventBus(Object scheduler) {
        this.publisher = new SimplePublisher<>();
    }

    @Override
    public void subscribe(Subscriber<Event> subscriber) {
        publisher.subscribe(subscriber);
    }

    @Override
    public void publish(Event event) {

        recorder.record(event);

        publisher.emit(event);
    }

}
