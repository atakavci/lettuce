package io.lettuce.core.slimstreams;

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
        SimplePublisher<Long> publisher = new SimplePublisher<>();

        // Emit elements in a separate thread to avoid blocking
        Thread emitter = new Thread(() -> {
            try {
                for (long i = 0; i < elements; i++) {
                    publisher.emit(i);
                }
                publisher.complete();
            } catch (Exception e) {
                publisher.error(e);
            }
        });
        emitter.setDaemon(true);
        emitter.start();

        return publisher;
    }

    @Override
    public Publisher<Long> createFailedPublisher() {
        SimplePublisher<Long> publisher = new SimplePublisher<>();
        publisher.error(new RuntimeException("Test failure"));
        return publisher;
    }

    @Override
    public long maxElementsFromPublisher() {
        return 1024;
    }

}
