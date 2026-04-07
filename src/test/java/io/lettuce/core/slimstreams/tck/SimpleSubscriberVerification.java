package io.lettuce.core.slimstreams.tck;

import org.reactivestreams.Subscriber;
import org.reactivestreams.tck.SubscriberBlackboxVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import io.lettuce.core.slimstreams.SimpleSubscriber;

/**
 * Reactive Streams TCK blackbox verification for {@link SimpleSubscriber}.
 *
 * @author Ali TAKAVCI
 */
@Test
public class SimpleSubscriberVerification extends SubscriberBlackboxVerification<Long> {

    public SimpleSubscriberVerification() {
        super(new TestEnvironment(1000));
    }

    @Override
    public Subscriber<Long> createSubscriber() {
        return new SimpleSubscriber<>(subscription -> {
            // Request initial demand
            subscription.request(1);
        }, element -> {
            // Process element - request more
        }, error -> {
            // Handle error
        }, () -> {
            // Handle completion
        });
    }

    @Override
    public Long createElement(int element) {
        return (long) element;
    }

}
