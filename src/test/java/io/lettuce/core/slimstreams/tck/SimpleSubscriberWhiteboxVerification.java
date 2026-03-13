package io.lettuce.core.slimstreams.tck;

import org.reactivestreams.Subscriber;
import org.reactivestreams.tck.SubscriberWhiteboxVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import io.lettuce.core.slimstreams.SimpleSubscriber;

/**
 * Reactive Streams TCK whitebox verification for {@link SimpleSubscriber}.
 *
 * @author Ali TAKAVCI
 */
@Test
public class SimpleSubscriberWhiteboxVerification extends SubscriberWhiteboxVerification<Long> {

    public SimpleSubscriberWhiteboxVerification() {
        super(new TestEnvironment(1000));
    }

    @Override
    public Subscriber<Long> createSubscriber(WhiteboxSubscriberProbe<Long> probe) {
        return new SimpleSubscriber<Long>(subscription -> {
            probe.registerOnSubscribe(new SubscriberWhiteboxVerification.SubscriberPuppet() {

                @Override
                public void triggerRequest(long elements) {
                    subscription.request(elements);
                }

                @Override
                public void signalCancel() {
                    subscription.cancel();
                }

            });
        }, element -> {
            probe.registerOnNext(element);
        }, error -> {
            probe.registerOnError(error);
        }, () -> {
            probe.registerOnComplete();
        });
    }

    @Override
    public Long createElement(int element) {
        return (long) element;
    }

}
