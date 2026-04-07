package io.lettuce.core.event;

import io.lettuce.core.slimstreams.DemandingSubscriber;

public class EventSubscriber<T extends Event> extends DemandingSubscriber<T> {

    public EventSubscriber(Class<T> eventType, NextHandler<T> nextHandler) {
        super(event -> {
            if (eventType.isInstance(event))
                nextHandler.onNext(event);
        });
    }

    public EventSubscriber(Class<T> eventType, SubscribeHandler subscribeHandler, NextHandler<T> nextHandler,
            ErrorHandler errorHandler, CompleteHandler completeHandler) {
        super(subscribeHandler, event -> {
            if (eventType.isInstance(event))
                nextHandler.onNext(event);
        }, errorHandler, completeHandler);
    }

    public static <T extends Event> EventSubscriber<T> forEvent(Class<T> eventType, NextHandler<T> nextHandler) {
        return new EventSubscriber<>(eventType, nextHandler);
    }

}
