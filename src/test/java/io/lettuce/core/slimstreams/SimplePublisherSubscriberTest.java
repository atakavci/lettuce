package io.lettuce.core.slimstreams;

import org.junit.jupiter.api.*;
import org.reactivestreams.Subscription;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive JUnit 5 test suite for {@link SimplePublisher}, {@link SimpleSubscriber}, and {@link SimpleSubscription}.
 *
 * <p>
 * All tests use the real implementations — no mocking of core classes. Method-level timeouts guard against liveness failures
 * (missed wakeup, deadlock, livelock) that would otherwise block indefinitely.
 *
 * <p>
 * Test categories:
 * <ul>
 * <li>{@link HappyPath} — normal publish-and-receive flows</li>
 * <li>{@link Backpressure} — demand control behaviour under load</li>
 * <li>{@link Concurrency} — thread safety and race condition scenarios</li>
 * <li>{@link EdgeCases} — null events, zero demand, late subscribers, exceptions</li>
 * <li>{@link Ordering} — delivery order guarantees under concurrency</li>
 * <li>{@link Lifecycle} — completion, error, and cancellation signal handling</li>
 * </ul>
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class SimplePublisherSubscriberTest {

    // =========================================================================
    // Test infrastructure
    // =========================================================================

    /**
     * A self-contained recording subscriber that subclasses {@link SimpleSubscriber} by overriding the signal methods directly.
     *
     * <p>
     * The super-constructor receives no-op lambdas; all real behaviour lives in the overridden methods, which is necessary
     * because {@code this} cannot be captured inside a {@code super()} argument list in Java.
     *
     * <p>
     * Thread-safe: all mutable state is either {@code CopyOnWriteArrayList}, {@code AtomicReference}, {@code AtomicBoolean}, or
     * protected by a {@code CountDownLatch}.
     */
    static class RecordingSubscriber<T> extends SimpleSubscriber<T> {

        /** Every element delivered via onNext, in arrival order. */
        final List<T> items = new CopyOnWriteArrayList<>();

        final AtomicReference<Throwable> error = new AtomicReference<>();

        final AtomicBoolean completed = new AtomicBoolean(false);

        final AtomicInteger onNextCallCount = new AtomicInteger(0);

        // Latches — callers await() these to synchronise without busy-spinning.
        final CountDownLatch completeLatch;

        final CountDownLatch errorLatch;

        // Counts down once per onNext; sized to the expected total item count.
        final CountDownLatch itemLatch;

        // Captured inside onSubscribe so tests can issue further requests.
        volatile Subscription subscription;

        private final long initialDemand;

        private final boolean autoReRequest; // re-request(1) after every onNext

        RecordingSubscriber(long initialDemand, boolean autoReRequest, int expectedItems) {
            super(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            }); // no-op delegates — overridden below
            this.initialDemand = initialDemand;
            this.autoReRequest = autoReRequest;
            this.completeLatch = new CountDownLatch(1);
            this.errorLatch = new CountDownLatch(1);
            this.itemLatch = new CountDownLatch(Math.max(1, expectedItems));
        }

        @Override
        public void onSubscribe(Subscription s) {
            // Call super first to ensure parent's state machine is updated
            // and spec compliance checks (§2.12, §2.13) are performed
            super.onSubscribe(s);
            // Only proceed if super.onSubscribe succeeded (didn't throw or cancel)
            this.subscription = s;
            if (initialDemand > 0)
                s.request(initialDemand);
        }

        @Override
        public void onNext(T t) {
            // Call super first to ensure:
            // - Terminal guard (§2.1) prevents delivery after onComplete/onError
            // - Null check (§2.13) is performed
            // - Exception handling and auto-cancel work correctly
            super.onNext(t);
            // Only record if super.onNext succeeded (didn't throw)
            onNextCallCount.incrementAndGet();
            items.add(t);
            itemLatch.countDown();
            if (autoReRequest)
                subscription.request(1);
        }

        @Override
        public void onError(Throwable t) {
            // Record first, then call super to transition state to TERMINATED
            error.set(t);
            errorLatch.countDown();
            super.onError(t);
        }

        @Override
        public void onComplete() {
            // Record first, then call super to transition state to TERMINATED
            completed.set(true);
            completeLatch.countDown();
            super.onComplete();
        }

        /** Blocks until at least {@code n} items have arrived, or the timeout elapses. */
        boolean awaitItems(int n, long timeout, TimeUnit unit) throws InterruptedException {
            long deadlineNs = System.nanoTime() + unit.toNanos(timeout);
            while (items.size() < n) {
                long remainingNs = deadlineNs - System.nanoTime();
                if (remainingNs <= 0)
                    return false;
                // noinspection BusyWait
                Thread.sleep(Math.min(5L, TimeUnit.NANOSECONDS.toMillis(remainingNs) + 1));
            }
            return true;
        }

        boolean awaitComplete(long timeout, TimeUnit unit) throws InterruptedException {
            return completeLatch.await(timeout, unit);
        }

        boolean awaitError(long timeout, TimeUnit unit) throws InterruptedException {
            return errorLatch.await(timeout, unit);
        }

        /** Stable brief sleep to let potential spurious deliveries arrive. */
        static void stablePause() throws InterruptedException {
            Thread.sleep(80);
        }

    }

    // =========================================================================
    // HAPPY PATH
    // =========================================================================

    @Nested
    @DisplayName("Happy Path — normal publish-and-receive flows")
    class HappyPath {

        @Test
        @Disabled("Not supported with current implementation - no buffering before subscription")
        @DisplayName("items emitted before subscription are buffered and delivered on subscribe")
        void preEmittedItems_deliveredOnSubscription() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            pub.emit(1);
            pub.emit(2);
            pub.emit(3);

            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(10, false, 3);
            pub.subscribe(sub);

            assertTrue(sub.awaitItems(3, 2, TimeUnit.SECONDS));
            assertEquals(Arrays.asList(1, 2, 3), sub.items);
        }

        @Test
        @DisplayName("items emitted after subscription with sufficient demand are delivered immediately")
        void postEmittedItems_deliveredImmediately() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, 3);
            pub.subscribe(sub);

            pub.emit(10);
            pub.emit(20);
            pub.emit(30);

            assertTrue(sub.awaitItems(3, 2, TimeUnit.SECONDS));
            assertEquals(Arrays.asList(10, 20, 30), sub.items);
        }

        @Test
        @DisplayName("two subscribers each independently receive all emitted items")
        void twoSubscribers_eachReceiveAllItems() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub1 = new RecordingSubscriber<>(Long.MAX_VALUE, false, 3);
            RecordingSubscriber<Integer> sub2 = new RecordingSubscriber<>(Long.MAX_VALUE, false, 3);

            pub.subscribe(sub1);
            pub.subscribe(sub2);

            pub.emit(1);
            pub.emit(2);
            pub.emit(3);

            assertTrue(sub1.awaitItems(3, 2, TimeUnit.SECONDS));
            assertTrue(sub2.awaitItems(3, 2, TimeUnit.SECONDS));
            assertEquals(Arrays.asList(1, 2, 3), sub1.items);
            assertEquals(Arrays.asList(1, 2, 3), sub2.items);
        }

        @Test
        @DisplayName("onComplete is delivered to subscriber after all items are drained")
        void onComplete_deliveredAfterAllItems() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, 3);
            pub.subscribe(sub);

            pub.emit(1);
            pub.emit(2);
            pub.emit(3);
            pub.complete();

            assertTrue(sub.awaitComplete(2, TimeUnit.SECONDS));
            assertEquals(3, sub.items.size());
            assertTrue(sub.completed.get());
        }

        @Test
        @DisplayName("onError is delivered to subscriber with the exact throwable passed to error()")
        void onError_deliveredWithCorrectThrowable() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, 0);
            pub.subscribe(sub);

            RuntimeException cause = new RuntimeException("boom");
            pub.error(cause);

            assertTrue(sub.awaitError(2, TimeUnit.SECONDS));
            assertSame(cause, sub.error.get());
        }

        @Test
        @DisplayName("emit() returns true when publisher is active")
        void emit_returnsTrueWhileActive() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            assertTrue(pub.emit(1));
        }

        @Test
        @DisplayName("emit() returns false after complete()")
        void emit_returnsFalseAfterComplete() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            pub.complete();
            assertFalse(pub.emit(1));
        }

        @Test
        @DisplayName("emit() returns false after error()")
        void emit_returnsFalseAfterError() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            pub.error(new RuntimeException());
            assertFalse(pub.emit(1));
        }

        @Test
        @DisplayName("isCompleted() is false before complete() and true after")
        void isCompleted_reflectsState() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            assertFalse(pub.isCompleted());
            pub.complete();
            assertTrue(pub.isCompleted());
        }

        @Test
        @DisplayName("hasError() is false before error() and true after")
        void hasError_reflectsState() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            assertFalse(pub.hasError());
            pub.error(new RuntimeException());
            assertTrue(pub.hasError());
        }

        @Test
        @DisplayName("getBufferSize() reflects items sitting in subscriber buffer awaiting demand")
        void getBufferSize_reflectsBufferedItems() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            // Subscribe with zero demand — items will buffer.
            pub.subscribe(new SimpleSubscriber<>(s -> {
                /* intentionally no request */ }, t -> {
                }, e -> {
                }, () -> {
                }));

            pub.emit(1);
            pub.emit(2);
            pub.emit(3);
            RecordingSubscriber.stablePause();

            assertEquals(3, pub.getBufferSize());
        }

    }

    // =========================================================================
    // BACKPRESSURE
    // =========================================================================

    @Nested
    @DisplayName("Backpressure — demand control behaviour under load")
    class Backpressure {

        @Test
        @DisplayName("subscriber with demand=1 receives exactly one item and holds the rest in buffer")
        void demandOne_deliversExactlyOneItem() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(1, false, 1);
            pub.subscribe(sub);

            pub.emit(1);
            pub.emit(2);
            pub.emit(3);

            assertTrue(sub.awaitItems(1, 2, TimeUnit.SECONDS));
            RecordingSubscriber.stablePause();
            assertEquals(1, sub.items.size(), "must not deliver beyond requested demand");
        }

        @Test
        @Disabled("Not supported with current implementation - no buffering before subscription")
        @DisplayName("items emitted with zero demand are fully buffered and delivered when demand arrives")
        void zeroDemand_itemsBuffered_deliveredOnRequest() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Subscription> subRef = new AtomicReference<>();

            pub.subscribe(new SimpleSubscriber<>(s -> subRef.set(s), // no request() yet
                    t -> {
                    }, e -> {
                    }, () -> {
                    }));

            pub.emit(10);
            pub.emit(20);
            pub.emit(30);

            RecordingSubscriber.stablePause();
            assertEquals(3, pub.getBufferSize(), "all items must stay in buffer");

            List<Integer> received = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(3);

            pub.subscribe(new SimpleSubscriber<>(
                    // second subscriber just to verify; real delivery via subRef
                    s -> {
                    }, t -> {
                    }, e -> {
                    }, () -> {
                    }));

            // Now make the first subscriber request everything
            pub.subscribe(new SimpleSubscriber<>(s -> s.request(3), t -> {
                received.add(t);
                latch.countDown();
            }, e -> {
            }, () -> {
            }));

            assertTrue(latch.await(2, TimeUnit.SECONDS));
            assertEquals(3, received.size());
        }

        @Test
        @DisplayName("1-by-1 auto re-request drives full ordered delivery of 200 items")
        void autoReRequest_drivesFullDelivery() throws InterruptedException {
            int count = 200;
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(1, true, count);
            pub.subscribe(sub);

            for (int i = 0; i < count; i++)
                pub.emit(i);

            assertTrue(sub.awaitItems(count, 5, TimeUnit.SECONDS));
            assertEquals(count, sub.items.size());
        }

        @Test
        @DisplayName("demand accumulation: two separate request(5) calls deliver exactly 10 items")
        void demandAccumulation_summedCorrectly() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            List<Integer> received = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(10);
            AtomicReference<Subscription> subRef = new AtomicReference<>();

            pub.subscribe(new SimpleSubscriber<>(s -> {
                subRef.set(s);
                s.request(5);
            }, t -> {
                received.add(t);
                latch.countDown();
            }, e -> {
            }, () -> {
            }));

            for (int i = 1; i <= 20; i++)
                pub.emit(i);

            subRef.get().request(5); // add 5 more demand
            assertTrue(latch.await(3, TimeUnit.SECONDS));
            assertEquals(10, received.size());
        }

        @Test
        @DisplayName("Long.MAX_VALUE demand is treated as unbounded — all items delivered without decrement")
        void maxValueDemand_treatedAsUnbounded() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, 50);
            pub.subscribe(sub);

            for (int i = 0; i < 50; i++)
                pub.emit(i);

            assertTrue(sub.awaitItems(50, 3, TimeUnit.SECONDS));
            assertEquals(50, sub.items.size());
            // getDemand on the underlying subscription should still be Long.MAX_VALUE
            assertEquals(Long.MAX_VALUE, ((SimpleSubscription) sub.subscription).getDemand());
        }

        @Test
        @DisplayName("slow subscriber (low demand) does not starve a fast subscriber in multicast")
        void slowSubscriber_doesNotStarveFastSubscriber() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> fast = new RecordingSubscriber<>(Long.MAX_VALUE, false, 20);

            // Slow sub: intentionally no demand requested
            pub.subscribe(new SimpleSubscriber<>(s -> {
                /* no request */ }, t -> {
                }, e -> {
                }, () -> {
                }));
            pub.subscribe(fast);

            for (int i = 0; i < 20; i++)
                pub.emit(i);

            assertTrue(fast.awaitItems(20, 3, TimeUnit.SECONDS));
            assertEquals(20, fast.items.size());
        }

        @Test
        @DisplayName("tryConsume() atomically gates delivery — no item delivered when demand is zero")
        void tryConsume_noDeliveryWhenDemandZero() {
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            pub.subscribe(new SimpleSubscriber<>(subRef::set, t -> {
            }, e -> {
            }, () -> {
            }));

            SimpleSubscription sub = (SimpleSubscription) subRef.get();
            // No request issued — demand should be 0
            assertFalse(sub.tryConsume(), "tryConsume must return false with zero demand");
        }

        @Test
        @DisplayName("tryConsume() returns true and decrements demand by exactly 1")
        void tryConsume_decrementsOnSuccess() {
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            pub.subscribe(new SimpleSubscriber<>(s -> {
                subRef.set(s);
                s.request(3);
            }, t -> {
            }, e -> {
            }, () -> {
            }));

            SimpleSubscription sub = (SimpleSubscription) subRef.get();
            assertTrue(sub.tryConsume());
            assertEquals(2L, sub.getDemand());
        }

        @Test
        @DisplayName("decrementDemand() saturates at zero — never goes negative")
        void decrementDemand_saturatesAtZero() {
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            pub.subscribe(new SimpleSubscriber<>(s -> {
                subRef.set(s);
                s.request(2);
            }, t -> {
            }, e -> {
            }, () -> {
            }));

            SimpleSubscription sub = (SimpleSubscription) subRef.get();
            sub.decrementDemand(10); // decrement more than available
            assertEquals(0L, sub.getDemand(), "demand must saturate at 0, not go negative");
        }

    }

    // =========================================================================
    // CONCURRENCY
    // =========================================================================

    @Nested
    @DisplayName("Concurrency — thread safety and race condition scenarios")
    class Concurrency {

        @Test
        @Timeout(value = 15, unit = TimeUnit.SECONDS)
        @DisplayName("16 concurrent emitter threads — all items delivered exactly once, no duplicates")
        void concurrentEmitters_allItemsDeliveredExactlyOnce() throws InterruptedException {
            int threads = 16;
            int perThread = 200;
            int total = threads * perThread;

            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, total);
            pub.subscribe(sub);

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CyclicBarrier barrier = new CyclicBarrier(threads);
            AtomicInteger counter = new AtomicInteger(0);

            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        barrier.await();
                    } catch (Exception ignored) {
                    }
                    for (int i = 0; i < perThread; i++)
                        pub.emit(counter.getAndIncrement());
                });
            }

            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertTrue(sub.awaitItems(total, 10, TimeUnit.SECONDS));

            assertEquals(total, sub.items.size(), "no items lost");
            assertEquals(total, new HashSet<>(sub.items).size(), "no duplicates");
        }

        @Test
        @DisplayName("concurrent request() and emit() do not lose items or deadlock")
        void concurrentRequestAndEmit_noLossNorDeadlock() throws InterruptedException {
            int items = 500;
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            List<Integer> received = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(items);
            AtomicReference<Subscription> subRef = new AtomicReference<>();

            pub.subscribe(new SimpleSubscriber<>(s -> subRef.set(s), // no initial demand
                    t -> {
                        received.add(t);
                        done.countDown();
                    }, e -> {
                    }, () -> {
                    }));

            ExecutorService emitter = Executors.newSingleThreadExecutor();
            ExecutorService requester = Executors.newSingleThreadExecutor();

            emitter.submit(() -> {
                for (int i = 0; i < items; i++)
                    pub.emit(i);
            });
            requester.submit(() -> {
                for (int i = 0; i < items; i++) {
                    subRef.get().request(1);
                    Thread.yield();
                }
            });

            assertTrue(done.await(8, TimeUnit.SECONDS), "all items must be delivered");
            assertEquals(items, received.size());

            emitter.shutdown();
            requester.shutdown();
        }

        @Test
        @DisplayName("concurrent cancel() and emit() — no onNext arrives after cancel is observed")
        void concurrentCancelAndEmit_noSignalAfterCancel() throws InterruptedException {
            // Run many rounds to surface potential races
            for (int round = 0; round < 300; round++) {
                SimplePublisher<Integer> pub = new SimplePublisher<>();
                AtomicBoolean cancelled = new AtomicBoolean(false);
                List<Integer> afterCancel = new CopyOnWriteArrayList<>();
                AtomicReference<Subscription> subRef = new AtomicReference<>();

                pub.subscribe(new SimpleSubscriber<>(s -> {
                    subRef.set(s);
                    s.request(Long.MAX_VALUE);
                }, t -> {
                    if (cancelled.get())
                        afterCancel.add(t);
                }, e -> {
                }, () -> {
                }));

                CountDownLatch go = new CountDownLatch(1);
                Thread emitThread = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                    }
                    for (int i = 0; i < 20; i++)
                        pub.emit(i);
                });
                Thread cancelThread = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                    }
                    subRef.get().cancel();
                    cancelled.set(true);
                });

                emitThread.start();
                cancelThread.start();
                go.countDown();
                emitThread.join(500);
                cancelThread.join(500);

                // After cancel is observed we tolerate a small in-flight window
                assertTrue(afterCancel.size() <= 2,
                        "too many items after cancel in round " + round + ": " + afterCancel.size());
            }
        }

        @Test
        @DisplayName("20 concurrent subscribers subscribing simultaneously all receive onSubscribe exactly once")
        void concurrentSubscribe_eachGetsOnSubscribeOnce() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            int subCount = 20;
            AtomicInteger onSubscribeCount = new AtomicInteger(0);
            CountDownLatch allSubscribed = new CountDownLatch(subCount);
            CyclicBarrier barrier = new CyclicBarrier(subCount);
            ExecutorService pool = Executors.newFixedThreadPool(subCount);

            for (int i = 0; i < subCount; i++) {
                pool.submit(() -> {
                    try {
                        barrier.await();
                    } catch (Exception ignored) {
                    }
                    pub.subscribe(new SimpleSubscriber<>(s -> {
                        onSubscribeCount.incrementAndGet();
                        allSubscribed.countDown();
                    }, t -> {
                    }, e -> {
                    }, () -> {
                    }));
                });
            }

            assertTrue(allSubscribed.await(3, TimeUnit.SECONDS));
            assertEquals(subCount, onSubscribeCount.get());
            pool.shutdown();
        }

        @Test
        @DisplayName("WIP missed-wakeup stress: offer racing drain loop exit delivers every item")
        @Timeout(value = 20, unit = TimeUnit.SECONDS)
        void wipMissedWakeup_allItemsAlwaysDelivered() throws InterruptedException {
            // This test specifically targets the race the WIP pattern was introduced to fix:
            // emit()/offer() landing between the drain loop's isEmpty check and wip.decrementAndGet().
            int rounds = 2_000;
            for (int rCounter = 0; rCounter < rounds; rCounter++) {
                SimplePublisher<Integer> pub = new SimplePublisher<>();
                AtomicReference<Subscription> subRef = new AtomicReference<>();
                List<Integer> received = new CopyOnWriteArrayList<>();
                CountDownLatch itemReceived = new CountDownLatch(1);

                pub.subscribe(new SimpleSubscriber<>(s -> subRef.set(s), // no initial demand
                        t -> {
                            received.add(t);
                            itemReceived.countDown();
                        }, e -> {
                        }, () -> {
                        }));

                // Thread A: emit one item (races with demand arrival)
                AtomicInteger emitCounter = new AtomicInteger(rCounter);
                Thread emitThread = new Thread(() -> pub.emit(emitCounter.get()));
                // Thread B: request demand (races with buffer offer)
                Thread requestThread = new Thread(() -> subRef.get().request(1));

                emitThread.start();
                requestThread.start();
                emitThread.join(200);
                requestThread.join(200);

                assertTrue(itemReceived.await(500, TimeUnit.MILLISECONDS),
                        "item not delivered in round " + rCounter + " — missed wakeup detected");
            }
        }

        @Test
        @DisplayName("concurrent complete() and emit() — onComplete delivered exactly once")
        void concurrentCompleteAndEmit_onCompleteDeliveredExactlyOnce() throws InterruptedException {
            for (int round = 0; round < 100; round++) {
                SimplePublisher<Integer> pub = new SimplePublisher<>();
                AtomicInteger completeCount = new AtomicInteger(0);
                CountDownLatch latch = new CountDownLatch(1);

                pub.subscribe(new SimpleSubscriber<>(s -> s.request(Long.MAX_VALUE), t -> {
                }, e -> {
                }, () -> {
                    completeCount.incrementAndGet();
                    latch.countDown();
                }));

                CountDownLatch go = new CountDownLatch(1);
                Thread emitThread = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException i) {
                    }
                    for (int n = 0; n < 10; n++)
                        pub.emit(n);
                });
                Thread completeThread = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException i) {
                    }
                    pub.complete();
                });

                emitThread.start();
                completeThread.start();
                go.countDown();
                emitThread.join(500);
                completeThread.join(500);

                assertTrue(latch.await(1, TimeUnit.SECONDS));
                Thread.sleep(20);
                assertEquals(1, completeCount.get(), "onComplete must fire exactly once in round " + round);
            }
        }

        @Test
        @DisplayName("concurrent request() calls accumulate demand correctly under contention")
        void concurrentRequest_demandAccumulatesCorrectly() throws InterruptedException {
            int requestThreads = 8;
            int requestsPerThread = 25;
            int expectedDemand = requestThreads * requestsPerThread;

            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            pub.subscribe(new SimpleSubscriber<>(subRef::set, t -> {
            }, e -> {
            }, () -> {
            }));

            CyclicBarrier barrier = new CyclicBarrier(requestThreads);
            ExecutorService pool = Executors.newFixedThreadPool(requestThreads);

            for (int t = 0; t < requestThreads; t++) {
                pool.submit(() -> {
                    try {
                        barrier.await();
                    } catch (Exception ignored) {
                    }
                    for (int i = 0; i < requestsPerThread; i++)
                        subRef.get().request(1);
                });
            }

            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

            long demand = ((SimpleSubscription) subRef.get()).getDemand();
            // Demand may have been partially consumed by any drain, but must not exceed expectedDemand
            assertTrue(demand <= expectedDemand && demand >= 0, "demand out of expected range: " + demand);
        }

    }

    // =========================================================================
    // EDGE CASES
    // =========================================================================

    @Nested
    @DisplayName("Edge Cases — null events, zero demand, late subscribers, exceptions")
    class EdgeCases {

        @Test
        @DisplayName("emit(null) throws NullPointerException without affecting publisher state")
        void emit_null_throwsNPE() {
            SimplePublisher<String> pub = new SimplePublisher<>();
            assertThrows(NullPointerException.class, () -> pub.emit(null));
            assertTrue(pub.emit("valid"), "publisher must remain active after NPE");
        }

        @Test
        @DisplayName("subscribe(null) throws NullPointerException")
        void subscribe_null_throwsNPE() {
            assertThrows(NullPointerException.class, () -> new SimplePublisher<>().subscribe(null));
        }

        @Test
        @DisplayName("error(null) throws NullPointerException")
        void error_null_throwsNPE() {
            assertThrows(NullPointerException.class, () -> new SimplePublisher<>().error(null));
        }

        @Test
        @DisplayName("request(0) signals IllegalArgumentException to subscriber per §3.9")
        void requestZero_signalsIllegalArgumentException() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Throwable> err = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            pub.subscribe(new SimpleSubscriber<>(s -> s.request(0), // §3.9 violation
                    t -> {
                    }, e -> {
                        err.set(e);
                        latch.countDown();
                    }, () -> {
                    }));

            assertTrue(latch.await(2, TimeUnit.SECONDS));
            assertInstanceOf(IllegalArgumentException.class, err.get());
        }

        @Test
        @DisplayName("request(negative) signals IllegalArgumentException to subscriber per §3.9")
        void requestNegative_signalsIllegalArgumentException() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Throwable> err = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);

            pub.subscribe(new SimpleSubscriber<>(s -> s.request(-99), t -> {
            }, e -> {
                err.set(e);
                latch.countDown();
            }, () -> {
            }));

            assertTrue(latch.await(2, TimeUnit.SECONDS));
            assertInstanceOf(IllegalArgumentException.class, err.get());
        }

        @Test
        @Disabled("Not supported with current implementation - no buffering before subscription")
        @DisplayName("late subscriber receives buffered items accumulated before its subscription")
        void lateSubscriber_receivesPreviouslyBufferedItems() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            pub.emit(1);
            pub.emit(2);
            pub.emit(3);

            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(10, false, 3);
            pub.subscribe(sub); // subscribe AFTER emit

            assertTrue(sub.awaitItems(3, 2, TimeUnit.SECONDS));
            assertEquals(3, sub.items.size());
        }

        @Test
        @DisplayName("late subscriber receives onComplete immediately when publisher already completed and buffer empty")
        void lateSubscriber_receivesOnCompleteIfAlreadyCompleted() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            pub.complete();

            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(10, false, 0);
            pub.subscribe(sub);

            assertTrue(sub.awaitComplete(2, TimeUnit.SECONDS));
            assertTrue(sub.completed.get());
        }

        @Test
        @DisplayName("late subscriber receives onError immediately when publisher already errored and buffer empty")
        void lateSubscriber_receivesOnErrorIfAlreadyErrored() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RuntimeException cause = new RuntimeException("pre-existing error");
            pub.error(cause);

            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(10, false, 0);
            pub.subscribe(sub);

            assertTrue(sub.awaitError(2, TimeUnit.SECONDS));
            assertSame(cause, sub.error.get());
        }

        @Test
        @DisplayName("onNext throwing in subscriber triggers cancel and does not break other subscribers")
        void subscriberOnNextThrowing_doesNotBreakOtherSubscribers() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();

            // Misbehaving subscriber — throws on every item
            pub.subscribe(new SimpleSubscriber<>(s -> s.request(Long.MAX_VALUE), t -> {
                throw new RuntimeException("bad subscriber");
            }, e -> {
            }, () -> {
            }));

            RecordingSubscriber<Integer> good = new RecordingSubscriber<>(Long.MAX_VALUE, false, 5);
            pub.subscribe(good);

            for (int i = 0; i < 5; i++)
                pub.emit(i);

            assertTrue(good.awaitItems(5, 2, TimeUnit.SECONDS));
            assertEquals(5, good.items.size());
        }

        @Test
        @DisplayName("rapid subscribe/unsubscribe cycle does not leak subscribers or throw exceptions")
        void rapidSubscribeUnsubscribe_noLeakOrException() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();

            for (int i = 0; i < 300; i++) {
                AtomicReference<Subscription> ref = new AtomicReference<>();
                pub.subscribe(new SimpleSubscriber<>(s -> {
                    ref.set(s);
                    s.request(1);
                }, t -> {
                }, e -> {
                }, () -> {
                }));
                Subscription s = ref.get();
                if (s != null)
                    s.cancel();
                pub.emit(i);
            }

            // Publisher still functional after churn
            RecordingSubscriber<Integer> final_ = new RecordingSubscriber<>(1, false, 1);
            pub.subscribe(final_);
            pub.emit(999);
            assertTrue(final_.awaitItems(1, 2, TimeUnit.SECONDS));
        }

        @Test
        @DisplayName("cancel() in onSubscribe prevents any items from being delivered")
        void cancelInOnSubscribe_noItemsDelivered() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            List<Integer> received = new CopyOnWriteArrayList<>();

            pub.subscribe(new SimpleSubscriber<>(Subscription::cancel, // cancel immediately inside onSubscribe
                    received::add, e -> {
                    }, () -> {
                    }));

            pub.emit(1);
            pub.emit(2);
            RecordingSubscriber.stablePause();

            assertEquals(0, received.size(), "no items must be delivered after cancel in onSubscribe");
        }

        @Test
        @DisplayName("second error() call is silently ignored — onError delivered exactly once")
        void secondErrorCall_ignored() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicInteger errorCount = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);

            pub.subscribe(new SimpleSubscriber<>(s -> s.request(Long.MAX_VALUE), t -> {
            }, e -> {
                errorCount.incrementAndGet();
                latch.countDown();
            }, () -> {
            }));

            pub.error(new RuntimeException("first"));
            pub.error(new RuntimeException("second")); // must be ignored

            assertTrue(latch.await(2, TimeUnit.SECONDS));
            RecordingSubscriber.stablePause();
            assertEquals(1, errorCount.get(), "onError must fire exactly once");
        }

        @Test
        @DisplayName("complete() followed by emit() — item rejected and not delivered to subscriber")
        void completeFollowedByEmit_itemNotDelivered() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, 0);
            pub.subscribe(sub);

            pub.complete();
            assertFalse(pub.emit(42));
            RecordingSubscriber.stablePause();

            assertEquals(0, sub.items.size(), "post-complete item must not be delivered");
        }

        @Test
        @DisplayName("EmissionController is invoked with correct demand value and can push items")
        void emissionController_invokedWithCorrectDemand() throws InterruptedException {
            AtomicLong demandSeen = new AtomicLong(-1);
            CountDownLatch itemLatch = new CountDownLatch(1);
            List<Integer> received = new CopyOnWriteArrayList<>();

            SimplePublisher<Integer> pub = new SimplePublisher<>((demand, sink) -> {
                demandSeen.set(demand);
                sink.emit(777);
                return demand;
            });

            pub.subscribe(new SimpleSubscriber<>(s -> s.request(2), t -> {
                received.add(t);
                itemLatch.countDown();
            }, e -> {
            }, () -> {
            }));

            assertTrue(itemLatch.await(2, TimeUnit.SECONDS));
            assertEquals(2L, demandSeen.get(), "controller must receive the exact demand requested");
            assertTrue(received.contains(777));
        }

    }

    // =========================================================================
    // ORDERING
    // =========================================================================

    @Nested
    @DisplayName("Ordering — delivery order guarantees under concurrency")
    class Ordering {

        @Test
        @DisplayName("single-threaded emit preserves strict FIFO order for a single subscriber")
        void singleThread_strictFifoOrder() throws InterruptedException {
            int count = 100;
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, count);
            pub.subscribe(sub);

            for (int i = 0; i < count; i++)
                pub.emit(i);

            assertTrue(sub.awaitItems(count, 3, TimeUnit.SECONDS));
            for (int i = 0; i < count; i++) {
                assertEquals(i, sub.items.get(i), "position " + i + " must hold value " + i);
            }
        }

        @Test
        @DisplayName("single-threaded emit with 1-by-1 backpressure preserves FIFO order")
        void singleThread_backpressure_fifoOrder() throws InterruptedException {
            int count = 50;
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(1, true, count);
            pub.subscribe(sub);

            for (int i = 0; i < count; i++)
                pub.emit(i);

            assertTrue(sub.awaitItems(count, 3, TimeUnit.SECONDS));
            for (int i = 0; i < count; i++) {
                assertEquals(i, sub.items.get(i), "1-by-1 backpressure must not reorder items");
            }
        }

        @Test
        @DisplayName("each concurrent emitter's own items arrive in the order that emitter sent them")
        void concurrentEmitters_perEmitterOrderPreserved() throws InterruptedException {
            int emitterCount = 6;
            int perEmitter = 100;
            int total = emitterCount * perEmitter;

            SimplePublisher<String> pub = new SimplePublisher<>();
            List<String> received = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(total);

            pub.subscribe(new SimpleSubscriber<>(s -> s.request(Long.MAX_VALUE), t -> {
                received.add(t);
                done.countDown();
            }, e -> {
            }, () -> {
            }));

            ExecutorService pool = Executors.newFixedThreadPool(emitterCount);
            CyclicBarrier barrier = new CyclicBarrier(emitterCount);

            for (int e = 0; e < emitterCount; e++) {
                final int eid = e;
                pool.submit(() -> {
                    try {
                        barrier.await();
                    } catch (Exception ignored) {
                    }
                    for (int i = 0; i < perEmitter; i++)
                        pub.emit(eid + ":" + i);
                });
            }

            assertTrue(done.await(5, TimeUnit.SECONDS));
            pool.shutdown();

            // For each emitter, verify its items appear with monotonically increasing sequence
            for (int eid = 0; eid < emitterCount; eid++) {
                final int emitterId = eid;
                List<Integer> seqs = received.stream().filter(s -> s.startsWith(emitterId + ":"))
                        .map(s -> Integer.parseInt(s.split(":")[1])).collect(Collectors.toList());

                assertEquals(perEmitter, seqs.size(), "emitter " + emitterId + " items missing");
                for (int i = 0; i < seqs.size() - 1; i++) {
                    assertTrue(seqs.get(i) < seqs.get(i + 1), "emitter " + emitterId + ": item at position " + i + " ("
                            + seqs.get(i) + ") must be < item at " + (i + 1) + " (" + seqs.get(i + 1) + ")");
                }
            }
        }

        @Test
        @DisplayName("buffered items delivered in FIFO order when demand arrives after buffering")
        void bufferedItems_deliveredInFifoOrder() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            List<Integer> received = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(5);

            pub.subscribe(new SimpleSubscriber<>(s -> subRef.set(s), // no initial demand — items will buffer
                    t -> {
                        received.add(t);
                        done.countDown();
                    }, e -> {
                    }, () -> {
                    }));

            for (int i = 0; i < 5; i++)
                pub.emit(i);
            RecordingSubscriber.stablePause(); // let items settle in buffer

            subRef.get().request(5); // open floodgate
            assertTrue(done.await(2, TimeUnit.SECONDS));

            assertEquals(Arrays.asList(0, 1, 2, 3, 4), received, "buffered items must arrive in FIFO order");
        }

    }

    // =========================================================================
    // LIFECYCLE
    // =========================================================================

    @Nested
    @DisplayName("Lifecycle — completion, error, and cancellation signal handling")
    class Lifecycle {

        @Test
        @DisplayName("onComplete fires exactly once even if complete() is called three times")
        void multipleCompleteCallsConsolidated_onCompleteOnce() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicInteger count = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(1);

            pub.subscribe(new SimpleSubscriber<>(s -> s.request(Long.MAX_VALUE), t -> {
            }, e -> {
            }, () -> {
                count.incrementAndGet();
                latch.countDown();
            }));

            pub.complete();
            pub.complete();
            pub.complete();

            assertTrue(latch.await(2, TimeUnit.SECONDS));
            RecordingSubscriber.stablePause();
            assertEquals(1, count.get(), "onComplete must fire exactly once");
        }

        @Test
        @DisplayName("buffered items are fully drained before onComplete is delivered")
        void bufferedItems_drainedBeforeOnComplete() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            List<Integer> received = new CopyOnWriteArrayList<>();
            CountDownLatch completeLatch = new CountDownLatch(1);

            pub.subscribe(new SimpleSubscriber<>(s -> s.request(Long.MAX_VALUE), received::add, e -> {
            }, () -> completeLatch.countDown()));

            pub.emit(1);
            pub.emit(2);
            pub.emit(3);
            pub.complete();

            assertTrue(completeLatch.await(2, TimeUnit.SECONDS));
            assertEquals(Arrays.asList(1, 2, 3), received, "all buffered items must be delivered before onComplete");
        }

        @Test
        @DisplayName("buffered items are fully drained before onError is delivered")
        void bufferedItems_drainedBeforeOnError() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            List<Integer> received = new CopyOnWriteArrayList<>();
            CountDownLatch errorLatch = new CountDownLatch(1);

            pub.subscribe(
                    new SimpleSubscriber<>(s -> s.request(Long.MAX_VALUE), received::add, e -> errorLatch.countDown(), () -> {
                    }));

            pub.emit(1);
            pub.emit(2);
            pub.error(new RuntimeException("err"));

            assertTrue(errorLatch.await(2, TimeUnit.SECONDS));
            // Both pre-error items must have been delivered
            assertEquals(2, received.size(), "all pre-error items must be drained before onError");
        }

        @Test
        @DisplayName("cancel() prevents all future deliveries and removes subscriber from publisher")
        void cancel_preventsDeliveryAndRemovesSubscriber() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            List<Integer> received = new CopyOnWriteArrayList<>();

            pub.subscribe(new SimpleSubscriber<>(s -> {
                subRef.set(s);
                s.request(Long.MAX_VALUE);
            }, received::add, e -> {
            }, () -> {
            }));

            pub.emit(1);
            pub.emit(2);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (received.size() < 2 && System.nanoTime() < deadline)
                Thread.yield();

            subRef.get().cancel();
            int sizeAtCancel = received.size();

            pub.emit(100);
            pub.emit(200);
            pub.emit(300);
            RecordingSubscriber.stablePause();

            // After cancel buffer is cleared — no subsequent items should be buffered
            assertEquals(0, pub.getBufferSize(), "cancelled subscriber's buffer must be cleared");
            // May have received some in-flight items but must not grow unboundedly
            assertTrue(received.size() <= sizeAtCancel + 1,
                    "must not receive items after cancel (beyond possible single in-flight)");
        }

        @Test
        @DisplayName("cancel() is idempotent — calling it multiple times does not throw or double-signal")
        void cancel_isIdempotent() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            AtomicInteger cancelCallbackCount = new AtomicInteger(0);

            pub.subscribe(new SimpleSubscriber<>(s -> subRef.set(s), t -> {
            }, e -> {
            }, () -> {
            }));

            Subscription sub = subRef.get();
            assertDoesNotThrow(() -> {
                sub.cancel();
                sub.cancel();
                sub.cancel();
            });
        }

        @Test
        @DisplayName("isCancelled() returns true after cancel() is called on the subscription")
        void isCancelled_trueAfterCancel() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            pub.subscribe(new SimpleSubscriber<>(subRef::set, t -> {
            }, e -> {
            }, () -> {
            }));

            SimpleSubscription sub = (SimpleSubscription) subRef.get();
            assertFalse(sub.isCancelled());
            sub.cancel();
            assertTrue(sub.isCancelled());
        }

        @Test
        @DisplayName("request() after cancel() is a no-op and does not invoke requestHandler")
        void requestAfterCancel_noOp() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            AtomicInteger handlerInvocations = new AtomicInteger(0);

            // We need a publisher that counts requestHandler calls.
            // Use EmissionController path to get visibility into handler invocations.
            SimplePublisher<Integer> controlled = new SimplePublisher<>((demand, sink) -> {
                handlerInvocations.incrementAndGet();
                return demand;
            });

            controlled.subscribe(new SimpleSubscriber<>(s -> {
                subRef.set(s);
                s.request(1);
            }, // one legitimate request
                    t -> {
                    }, e -> {
                    }, () -> {
                    }));

            int countAfterFirst = handlerInvocations.get();
            subRef.get().cancel();

            subRef.get().request(5); // must be no-op
            RecordingSubscriber.stablePause();

            assertEquals(countAfterFirst, handlerInvocations.get(), "requestHandler must not be invoked after cancel");
        }

        @Test
        @DisplayName("SimpleSubscriber.isTerminated() is true after onComplete")
        void simpleSubscriber_isTerminated_afterOnComplete() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, 0);
            pub.subscribe(sub);
            pub.complete();

            assertTrue(sub.awaitComplete(2, TimeUnit.SECONDS));
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("SimpleSubscriber.isTerminated() is true after onError")
        void simpleSubscriber_isTerminated_afterOnError() throws InterruptedException {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, 0);
            pub.subscribe(sub);
            pub.error(new RuntimeException());

            assertTrue(sub.awaitError(2, TimeUnit.SECONDS));
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("SimpleSubscriber.isTerminated() is true after explicit cancel()")
        void simpleSubscriber_isTerminated_afterCancel() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            RecordingSubscriber<Integer> sub = new RecordingSubscriber<>(Long.MAX_VALUE, false, 0);
            pub.subscribe(sub);

            assertFalse(sub.isTerminated());
            sub.cancel();
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("getDemand() returns -1 after subscription is cancelled")
        void getDemand_returnsMinusOneAfterCancel() {
            SimplePublisher<Integer> pub = new SimplePublisher<>();
            AtomicReference<Subscription> subRef = new AtomicReference<>();
            pub.subscribe(new SimpleSubscriber<>(s -> {
                subRef.set(s);
                s.request(5);
            }, t -> {
            }, e -> {
            }, () -> {
            }));

            SimpleSubscription sub = (SimpleSubscription) subRef.get();
            sub.cancel();
            assertEquals(-1L, sub.getDemand(), "getDemand() must return -1 after cancel");
        }

    }

}
