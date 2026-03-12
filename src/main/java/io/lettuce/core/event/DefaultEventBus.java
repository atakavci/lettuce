package io.lettuce.core.event;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

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

    private final Sinks.Many<Event> bus;

    private final Scheduler scheduler;

    private final EventRecorder recorder = EventRecorder.getInstance();

    private final SimplePublisher<Event> publisher;

    private static final SubscribeHandler subscribeHandler = s -> {
    };

    private static final ErrorHandler errorHandler = error -> {
    };

    private static final CompleteHandler completeHandler = () -> {
    };

    public DefaultEventBus(Scheduler scheduler) {
        this.publisher = new SimplePublisher<>();
        this.bus = Sinks.many().multicast().directBestEffort();
        this.scheduler = scheduler;
    }

    @Override
    public Flux<Event> get() {
        return bus.asFlux().onBackpressureDrop().publishOn(scheduler);
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
        Sinks.EmitResult emitResult;

        while ((emitResult = bus.tryEmitNext(event)) == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
            // busy-loop
        }

        if (emitResult != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            emitResult.orThrow();
        }
    }

}
