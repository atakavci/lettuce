package io.lettuce.core.event;

import org.reactivestreams.Subscription;

import io.lettuce.core.event.jfr.EventRecorder;
import io.lettuce.core.slimstreams.SimplePublisher;
import io.lettuce.core.slimstreams.SimpleSubscriber;
import io.lettuce.core.slimstreams.SimpleSubscriber.CompleteHandler;
import io.lettuce.core.slimstreams.SimpleSubscriber.ErrorHandler;
import io.lettuce.core.slimstreams.SimpleSubscriber.NextHandler;
import io.lettuce.core.slimstreams.SimpleSubscriber.SubscribeHandler;

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

    private static final SubscribeHandler subscribeHandler = s -> {
    };

    private static final ErrorHandler errorHandler = error -> {
    };

    private static final CompleteHandler completeHandler = () -> {
    };

    public DefaultEventBus(Object scheduler) {
        this.publisher = new SimplePublisher<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Event> Subscription subscribe(Class<T> eventType, NextHandler<T> eventHandler) {

        NextHandler<Event> n = next -> {
            if (eventType.isInstance(next)) {
                eventHandler.onNext((T) next);
            }
        };

        SimpleSubscriber<Event> simpleSubscriber = new SimpleSubscriber<Event>(subscribeHandler, n, errorHandler,
                completeHandler) {

            @Override
            public void onNext(Event event) {
                super.onNext(event);
                getSubscription().request(1);
            }

        };

        publisher.subscribe((SimpleSubscriber<Event>) simpleSubscriber);
        simpleSubscriber.getSubscription().request(1);
        return simpleSubscriber.getSubscription();
    }

    @Override
    public void publish(Event event) {

        recorder.record(event);

        publisher.emit(event);
    }

}
