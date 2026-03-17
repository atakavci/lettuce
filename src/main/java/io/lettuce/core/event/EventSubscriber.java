package io.lettuce.core.event;

import org.reactivestreams.Subscription;
import io.lettuce.core.slimstreams.SimpleSubscriber;

public class EventSubscriber extends SimpleSubscriber<Event> {

    private static final SubscribeHandler noopSubscribeHandler = s -> {
    };

    private static final ErrorHandler noopErrorHandler = error -> {
    };

    private static final CompleteHandler noopCompleteHandler = () -> {
    };

    public EventSubscriber(NextHandler<Event> nextHandler) {
        this(noopSubscribeHandler, nextHandler, noopErrorHandler, noopCompleteHandler);
    }

    public EventSubscriber(SubscribeHandler subscribeHandler, NextHandler<Event> nextHandler, ErrorHandler errorHandler,
            CompleteHandler completeHandler) {
        super(subscribeHandler, nextHandler, errorHandler, completeHandler);
    }

    @Override
    public void onSubscribe(Subscription s) {
        super.onSubscribe(s);
        s.request(1);
    }

    @Override
    public void onNext(Event event) {
        super.onNext(event);
        getSubscription().request(1);
    }

    public static <T extends Event> EventSubscriber forEvent(Class<T> eventType, NextHandler<T> nextHandler) {
        return new TypedEventSubscriber<>(eventType, nextHandler);
    }

    public static class TypedEventSubscriber<T extends Event> extends EventSubscriber {

        private final Class<T> eventType;

        @SuppressWarnings("unchecked")
        public TypedEventSubscriber(Class<T> eventType, NextHandler<T> nextHandler) {
            super((NextHandler<Event>) nextHandler);
            this.eventType = eventType;
        }

        @SuppressWarnings("unchecked")
        public TypedEventSubscriber(Class<T> eventType, SubscribeHandler subscribeHandler, NextHandler<T> nextHandler,
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

    }

}
