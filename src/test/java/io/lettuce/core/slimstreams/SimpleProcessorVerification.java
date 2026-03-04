package io.lettuce.core.slimstreams;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.IdentityProcessorVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Reactive Streams TCK verification for {@link SimpleProcessor}.
 *
 * @author Lettuce Contributors
 */
@Test
public class SimpleProcessorVerification extends IdentityProcessorVerification<Long> {

    private ExecutorService executorService;

    public SimpleProcessorVerification() {
        super(new TestEnvironment(1000), 1000, 1000);
    }

    @BeforeClass
    public void before() {
        executorService = Executors.newCachedThreadPool();
    }

    @AfterClass
    public void after() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Override
    public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
        return new SimpleProcessor<>();
    }

    @Override
    public Publisher<Long> createFailedPublisher() {
        SimplePublisher<Long> publisher = new SimplePublisher<>();
        publisher.error(new RuntimeException("Test failure"));
        return publisher;
    }

    @Override
    public ExecutorService publisherExecutorService() {
        return executorService;
    }

    @Override
    public Long createElement(int element) {
        return (long) element;
    }

    @Override
    public Publisher<Long> createHelperPublisher(long elements) {
        SimplePublisher<Long> publisher = new SimplePublisher<>();

        executorService.execute(() -> {
            try {
                for (long i = 0; i < elements; i++) {
                    publisher.emit(i);
                }
                publisher.complete();
            } catch (Exception e) {
                publisher.error(e);
            }
        });

        return publisher;
    }

    @Override
    public long maxSupportedSubscribers() {
        // SimpleProcessor supports only one subscriber (unicast)
        return 1;
    }

}
