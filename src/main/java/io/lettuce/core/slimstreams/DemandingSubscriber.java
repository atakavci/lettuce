package io.lettuce.core.slimstreams;

import org.reactivestreams.Subscription;

public class DemandingSubscriber<T> extends SimpleSubscriber<T> {

    private static final SubscribeHandler noopSubscribeHandler = s -> {
    };

    private static final ErrorHandler noopErrorHandler = error -> {
    };

    private static final CompleteHandler noopCompleteHandler = () -> {
    };

    public DemandingSubscriber(NextHandler<T> nextHandler) {
        this(noopSubscribeHandler, nextHandler, noopErrorHandler, noopCompleteHandler);
    }

    public DemandingSubscriber(SubscribeHandler subscribeHandler, NextHandler<T> nextHandler, ErrorHandler errorHandler,
            CompleteHandler completeHandler) {
        super(subscribeHandler, nextHandler, errorHandler, completeHandler);
    }

    @Override
    public void onSubscribe(Subscription s) {
        super.onSubscribe(s);
        s.request(1);
    }

    @Override
    public void onNext(T event) {
        super.onNext(event);
        getSubscription().request(1);
    }

}
