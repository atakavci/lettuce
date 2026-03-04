package io.lettuce.core.slimstreams;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating the usage of the simple reactive-streams implementations.
 *
 * @author Lettuce Contributors
 */
public class SimpleReactiveStreamsExample {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Simple Reactive Streams Example ===\n");

        // Example 1: Simple Publisher and Subscriber
        publisherSubscriberExample();

        // Example 2: Processor (transformation)
        processorExample();

        // Example 3: Backpressure handling
        backpressureExample();
    }

    private static void publisherSubscriberExample() throws InterruptedException {
        System.out.println("1. Publisher-Subscriber Example:");
        System.out.println("---------------------------------");

        SimplePublisher<String> publisher = new SimplePublisher<>();
        List<String> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        SimpleSubscriber<String> subscriber = new SimpleSubscriber<>(subscription -> {
            System.out.println("Subscribed! Requesting 10 items...");
            subscription.request(10);
        }, element -> {
            System.out.println("Received: " + element);
            received.add(element);
        }, error -> {
            System.err.println("Error: " + error.getMessage());
            latch.countDown();
        }, () -> {
            System.out.println("Completed!");
            latch.countDown();
        });

        publisher.subscribe(subscriber);

        // Emit some elements
        publisher.emit("Hello");
        publisher.emit("Reactive");
        publisher.emit("Streams");
        publisher.complete();

        latch.await(5, TimeUnit.SECONDS);
        System.out.println("Total received: " + received.size() + " items\n");
    }

    private static void processorExample() throws InterruptedException {
        System.out.println("2. Processor Example (String to Uppercase):");
        System.out.println("--------------------------------------------");

        SimplePublisher<String> publisher = new SimplePublisher<>();
        SimpleProcessor<String, String> processor = new SimpleProcessor<>(String::toUpperCase);
        List<String> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        // Connect publisher to processor
        publisher.subscribe(processor);

        // Connect processor to subscriber
        SimpleSubscriber<String> subscriber = new SimpleSubscriber<>(subscription -> {
            System.out.println("Subscribed to processor! Requesting 10 items...");
            subscription.request(10);
        }, element -> {
            System.out.println("Received transformed: " + element);
            received.add(element);
        }, error -> {
            System.err.println("Error: " + error.getMessage());
            latch.countDown();
        }, () -> {
            System.out.println("Completed!");
            latch.countDown();
        });

        processor.subscribe(subscriber);

        // Emit some elements
        publisher.emit("hello");
        publisher.emit("reactive");
        publisher.emit("streams");
        publisher.complete();

        latch.await(5, TimeUnit.SECONDS);
        System.out.println("Total transformed: " + received.size() + " items\n");
    }

    private static void backpressureExample() throws InterruptedException {
        System.out.println("3. Backpressure Example:");
        System.out.println("------------------------");

        SimplePublisher<Integer> publisher = new SimplePublisher<>();
        List<Integer> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        BackpressureSubscriber subscriber = new BackpressureSubscriber(received, latch);

        publisher.subscribe(subscriber);

        // Emit elements (some will be buffered due to backpressure)
        for (int i = 1; i <= 5; i++) {
            System.out.println("Emitting: " + i + " (buffered: " + publisher.getBufferSize() + ")");
            publisher.emit(i);
        }
        publisher.complete();

        latch.await(5, TimeUnit.SECONDS);
        System.out.println("Total received: " + received.size() + " items\n");
    }

    private static class BackpressureSubscriber extends SimpleSubscriber<Integer> {

        private final List<Integer> received;

        BackpressureSubscriber(List<Integer> received, CountDownLatch latch) {
            super(subscription -> {
                System.out.println("Subscribed! Requesting 2 items initially...");
                subscription.request(2);
            }, element -> {
            }, error -> {
                System.err.println("Error: " + error.getMessage());
                latch.countDown();
            }, () -> {
                System.out.println("Completed!");
                latch.countDown();
            });
            this.received = received;
        }

        @Override
        public void onNext(Integer element) {
            System.out.println("Received: " + element);
            received.add(element);

            // Request more after processing each element
            if (received.size() < 5) {
                System.out.println("Requesting 1 more item...");
                getSubscription().request(1);
            }
            super.onNext(element);
        }

    }

}
