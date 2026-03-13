package io.lettuce.core.slimstreams.tck;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.IdentityProcessorVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.Test;

import io.lettuce.core.slimstreams.EmissionController;
import io.lettuce.core.slimstreams.EmissionSink;
import io.lettuce.core.slimstreams.SimpleProcessor;
import io.lettuce.core.slimstreams.SimplePublisher;

/**
 * Reactive Streams TCK verification for {@link SimpleProcessor}.
 * <p>
 * Verifies that {@link SimpleProcessor} correctly implements the Reactive Streams specification by extending
 * {@link IdentityProcessorVerification}, which tests the processor both as a {@link org.reactivestreams.Subscriber} and as a
 * {@link Publisher}.
 *
 * @author Ali TAKAVCI
 */
@Test
public class SimpleProcessorVerification extends IdentityProcessorVerification<Long> {

    public SimpleProcessorVerification() {
        super(new TestEnvironment());
    }

    /**
     * Creates a new {@link SimpleProcessor} identity instance for each test. Uses {@link SimpleProcessor#identity()} since TCK
     * verifies pass-through behaviour.
     *
     * @param bufferSize the buffer size to use for the processor
     */
    @Override
    public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
        // Create identity processor without EmissionController
        // The processor should pass through elements from upstream, not generate them
        return new SimpleProcessor<>(element -> element);
    }

    /**
     * Provides a publisher of {@code Long} elements for upstream subscription tests. Returns {@code null} to let the TCK use
     * its own default publisher.
     *
     * @param elements the number of elements the publisher should emit
     */
    @Override
    public Long createElement(int element) {
        return (long) element;
    }

    @Override
    public ExecutorService publisherExecutorService() {
        return Executors.newCachedThreadPool();
    }

    @Override
    public Publisher<Long> createHelperPublisher(long elements) {
        return new SimplePublisher<>(createEmissionController(elements));
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

    /**
     * Provides a publisher that immediately terminates with an error. Used by the TCK to verify error propagation through the
     * processor.
     */
    @Override
    public Publisher<Long> createFailedPublisher() {
        return subscriber -> {
            SimplePublisher<Long> publisher = new SimplePublisher<>();
            publisher.subscribe(subscriber);
            publisher.error(new RuntimeException("failed publisher"));
        };
    }

}
