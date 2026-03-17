package io.lettuce.core.event;

import io.lettuce.core.slimstreams.DemandingSubscriber;

public class  EventSubscriber<T extends Event> extends DemandingSubscriber<Event> {

    private final Class<T> eventType;

    public EventSubscriber(Class<T> eventType, NextHandler<T> nextHandler) {
        super((NextHandler<Event>) nextHandler);
        this.eventType = eventType;
    }

    public EventSubscriber(Class<T> eventType, SubscribeHandler subscribeHandler, NextHandler<T> nextHandler,
            ErrorHandler errorHandler, CompleteHandler completeHandler) {
        super(subscribeHandler, (NextHandler<Event>) nextHandler, errorHandler, completeHandler);
        this.eventType = eventType;
    }

    @Override
    public void onNext(Event event) {
        if (eventType.isInstance(event)) {
            super.onNext(event);
        }
    }

    public static <T extends Event> EventSubscriber<T> forEvent(Class<T> eventType, NextHandler<T> nextHandler) {
        return new EventSubscriber<>(eventType, nextHandler);
    }

}
