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
        SimplePublisher<Long> publisher = new SimplePublisher<>(true);

        // Emit elements in a separate thread to avoid blocking
        Thread emitter = new Thread(() -> {
            try {
                for (long i = 0; i < elements; i++) {
                    // Emit with backpressure: wait if buffer is too large
                    while (publisher.getBufferSize() > 1024) {
                        if (Thread.interrupted()) {
                            return;
                        }
                        Thread.sleep(1);
                    }
                    publisher.emit(i);
                }
                publisher.complete();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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

}
