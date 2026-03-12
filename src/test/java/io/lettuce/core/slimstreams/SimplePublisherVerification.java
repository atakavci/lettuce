package io.lettuce.core.slimstreams;

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

/**
 * Reactive Streams TCK verification for {@link SimplePublisher}.
 *
 * @author Lettuce Contributors
 */
@Test
public class SimplePublisherVerification extends PublisherVerification<Long> {

    public SimplePublisherVerification() {
        super(new TestEnvironment(1000));
    }

    @Override
    public Publisher<Long> createPublisher(long elements) {
        SimplePublisher<Long> publisher = new SimplePublisher<>(createEmissionController(elements));
        return publisher;
    }

    private EmissionController<Long> createEmissionController(long maxElements) {

        return new EmissionController<Long>() {
            private AtomicLong totalEmitted = new AtomicLong();

            @Override
            public long onDemand(long requestedAmount, EmissionSink<Long> sink) {
                long limitedEmission = maxElements > 8196 ? 8196 : maxElements;
                long emitted = 0;
                while (emitted < requestedAmount && totalEmitted.get() < limitedEmission) {
                    long currentTotal = totalEmitted.getAndIncrement();
                    if (currentTotal < limitedEmission) {
                        sink.emit(currentTotal);
                        emitted++;
                        boolean lastEmit = currentTotal + 1 == limitedEmission;
                        if (lastEmit) {
                            sink.complete();
                        }
                    } else {
                        return emitted;
                    }
                }
                return emitted;
            }
        };
    }

    @Override
    public Publisher<Long> createFailedPublisher() {
        SimplePublisher<Long> publisher = new SimplePublisher<>();
        publisher.error(new RuntimeException("Test failure"));
        return publisher;
    }

    @Override
    public long maxElementsFromPublisher() {
        return Integer.MAX_VALUE;
    }

}
