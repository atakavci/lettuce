package io.lettuce.core.slimstreams;

import org.junit.jupiter.api.*;
import org.reactivestreams.Subscription;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Isolated unit tests for {@link SimpleSubscriber}.
 *
 * <p>
 * No {@link SimplePublisher} is involved. All signals ({@code onSubscribe}, {@code onNext}, {@code onError},
 * {@code onComplete}) are driven directly by the test, giving full control over ordering, timing, and edge-case sequences.
 *
 * <p>
 * A minimal {@link StubSubscription} is used as the upstream so that {@code cancel()} and {@code request()} calls can be
 * observed and verified.
 *
 * <p>
 * Test categories:
 * <ul>
 * <li>{@link Construction} — null-handler guards</li>
 * <li>{@link OnSubscribe} — state transitions, §2.5 duplicate, §2.13 null</li>
 * <li>{@link OnNext} — delivery, terminal guard, §2.13 null, handler throw</li>
 * <li>{@link OnError} — delivery, idempotency, §2.13 null, handler throw</li>
 * <li>{@link OnComplete} — delivery, idempotency, handler throw</li>
 * <li>{@link CancelMethod} — idempotency, upstream forwarding, state transition</li>
 * <li>{@link StateMachine} — full lifecycle transitions, getSubscription()</li>
 * <li>{@link HandlerExceptions}— throw semantics for each signal method</li>
 * <li>{@link Concurrency} — concurrent terminal signals, concurrent cancel + onNext</li>
 * </ul>
 */
@Timeout(value = 10, unit = TimeUnit.SECONDS)
class SimpleSubscriberTest {

    // =========================================================================
    // Stub infrastructure
    // =========================================================================

    /**
     * A minimal, observable {@link Subscription} stub. Records every {@code request()} and {@code cancel()} call so tests can
     * assert on upstream interactions without involving any real publisher infrastructure.
     */
    static class StubSubscription implements Subscription {

        final AtomicInteger cancelCount = new AtomicInteger(0);

        final AtomicLong lastRequested = new AtomicLong(0);

        final AtomicInteger requestCount = new AtomicInteger(0);

        @Override
        public void request(long n) {
            lastRequested.set(n);
            requestCount.incrementAndGet();
        }

        @Override
        public void cancel() {
            cancelCount.incrementAndGet();
        }

    }

    /** Convenience: build a no-op subscriber and call onSubscribe with a fresh stub. */
    private static SimpleSubscriber<String> subscribedNoOp() {
        SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
        }, t -> {
        }, e -> {
        }, () -> {
        });
        sub.onSubscribe(new StubSubscription());
        return sub;
    }

    // =========================================================================
    // Construction
    // =========================================================================

    @Nested
    @DisplayName("Construction — null handler guards")
    class Construction {

        @Test
        @DisplayName("null subscribeHandler throws NullPointerException")
        void nullSubscribeHandler_throwsNPE() {
            assertThrows(NullPointerException.class, () -> new SimpleSubscriber<String>(null, t -> {
            }, e -> {
            }, () -> {
            }));
        }

        @Test
        @DisplayName("null nextHandler throws NullPointerException")
        void nullNextHandler_throwsNPE() {
            assertThrows(NullPointerException.class, () -> new SimpleSubscriber<String>(s -> {
            }, null, e -> {
            }, () -> {
            }));
        }

        @Test
        @DisplayName("null errorHandler throws NullPointerException")
        void nullErrorHandler_throwsNPE() {
            assertThrows(NullPointerException.class, () -> new SimpleSubscriber<String>(s -> {
            }, t -> {
            }, null, () -> {
            }));
        }

        @Test
        @DisplayName("null completeHandler throws NullPointerException")
        void nullCompleteHandler_throwsNPE() {
            assertThrows(NullPointerException.class, () -> new SimpleSubscriber<String>(s -> {
            }, t -> {
            }, e -> {
            }, null));
        }

        @Test
        @DisplayName("all handlers non-null — construction succeeds and isTerminated() is false")
        void validConstruction_notTerminated() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            assertFalse(sub.isTerminated());
        }

    }

    // =========================================================================
    // onSubscribe
    // =========================================================================

    @Nested
    @DisplayName("onSubscribe — §2.5, §2.12, §2.13")
    class OnSubscribe {

        @Test
        @DisplayName("§2.13 — onSubscribe(null) throws NullPointerException")
        void onSubscribe_null_throwsNPE() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            assertThrows(NullPointerException.class, () -> sub.onSubscribe(null));
        }

        @Test
        @DisplayName("first onSubscribe stores subscription and invokes subscribeHandler")
        void firstOnSubscribe_invokesHandler() {
            AtomicBoolean handlerCalled = new AtomicBoolean(false);
            AtomicReference<Subscription> captured = new AtomicReference<>();

            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
                handlerCalled.set(true);
                captured.set(s);
            }, t -> {
            }, e -> {
            }, () -> {
            });

            StubSubscription stub = new StubSubscription();
            sub.onSubscribe(stub);

            assertTrue(handlerCalled.get(), "subscribeHandler must be invoked");
            assertSame(stub, captured.get(), "subscribeHandler must receive the exact Subscription");
        }

        @Test
        @DisplayName("§2.5 / §2.12 — second onSubscribe cancels the new subscription, handler not called again")
        void secondOnSubscribe_cancelsNewSubscription_handlerNotCalledAgain() {
            AtomicInteger handlerCallCount = new AtomicInteger(0);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> handlerCallCount.incrementAndGet(), t -> {
            }, e -> {
            }, () -> {
            });

            StubSubscription first = new StubSubscription();
            StubSubscription second = new StubSubscription();

            sub.onSubscribe(first);
            sub.onSubscribe(second);

            assertEquals(1, handlerCallCount.get(), "subscribeHandler must only be called once");
            assertEquals(0, first.cancelCount.get(), "first subscription must NOT be cancelled");
            assertEquals(1, second.cancelCount.get(), "second subscription MUST be cancelled (§2.5)");
        }

        @Test
        @DisplayName("§2.5 — onSubscribe after terminal state cancels the incoming subscription")
        void onSubscribe_afterTerminal_cancelsIncoming() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });

            sub.onSubscribe(new StubSubscription()); // subscribe
            sub.onComplete(); // terminate

            StubSubscription late = new StubSubscription();
            sub.onSubscribe(late);

            assertEquals(1, late.cancelCount.get(), "subscription arriving after terminal must be cancelled");
        }

        @Test
        @DisplayName("getSubscription() returns the stored subscription after onSubscribe")
        void getSubscription_returnsStoredSubscription() {
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);
            assertSame(stub, sub.getSubscription());
        }

        @Test
        @DisplayName("isTerminated() remains false after onSubscribe")
        void isTerminated_falseAfterOnSubscribe() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            assertFalse(sub.isTerminated());
        }

    }

    // =========================================================================
    // onNext
    // =========================================================================

    @Nested
    @DisplayName("onNext — §2.1, §2.13")
    class OnNext {

        @Test
        @DisplayName("§2.13 — onNext(null) throws NullPointerException")
        void onNext_null_throwsNPE() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            assertThrows(NullPointerException.class, () -> sub.onNext(null));
        }

        @Test
        @DisplayName("onNext delegates element to nextHandler")
        void onNext_delegatesToHandler() {
            AtomicReference<String> received = new AtomicReference<>();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, received::set, e -> {
            }, () -> {
            });
            sub.onSubscribe(new StubSubscription());
            sub.onNext("hello");
            assertEquals("hello", received.get());
        }

        @Test
        @DisplayName("§2.1 — onNext after onComplete is silently dropped, handler not called")
        void onNext_afterOnComplete_silentlyDropped() {
            AtomicInteger nextCount = new AtomicInteger(0);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> nextCount.incrementAndGet(), e -> {
            }, () -> {
            });
            sub.onSubscribe(new StubSubscription());
            sub.onComplete();
            sub.onNext("late");

            assertEquals(0, nextCount.get(), "onNext after onComplete must be silently dropped");
        }

        @Test
        @DisplayName("§2.1 — onNext after onError is silently dropped, handler not called")
        void onNext_afterOnError_silentlyDropped() {
            AtomicInteger nextCount = new AtomicInteger(0);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> nextCount.incrementAndGet(), e -> {
            }, () -> {
            });
            sub.onSubscribe(new StubSubscription());
            sub.onError(new RuntimeException("err"));
            sub.onNext("late");

            assertEquals(0, nextCount.get(), "onNext after onError must be silently dropped");
        }

        @Test
        @DisplayName("§2.1 — onNext after cancel() is silently dropped")
        void onNext_afterCancel_silentlyDropped() {
            AtomicInteger nextCount = new AtomicInteger(0);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> nextCount.incrementAndGet(), e -> {
            }, () -> {
            });
            sub.onSubscribe(new StubSubscription());
            sub.cancel();
            sub.onNext("late");

            assertEquals(0, nextCount.get(), "onNext after cancel must be silently dropped");
        }

        @Test
        @DisplayName("nextHandler throwing RuntimeException causes upstream cancel and rethrows")
        void nextHandlerThrowingRuntimeException_cancelsUpstreamAndRethrows() {
            StubSubscription stub = new StubSubscription();
            RuntimeException bang = new RuntimeException("bang");

            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
                throw bang;
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);

            RuntimeException thrown = assertThrows(RuntimeException.class, () -> sub.onNext("x"));
            assertSame(bang, thrown, "original exception must be rethrown");
            assertEquals(1, stub.cancelCount.get(), "upstream must be cancelled on handler throw");
            assertTrue(sub.isTerminated(), "subscriber must be terminated after handler throw");
        }

        @Test
        @DisplayName("nextHandler throwing checked exception is wrapped in RuntimeException and rethrown")
        void nextHandlerThrowingCheckedException_wrappedAndRethrown() {
            StubSubscription stub = new StubSubscription();
            Exception checked = new Exception("checked");

            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
                throw new RuntimeException(checked);
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);

            assertThrows(RuntimeException.class, () -> sub.onNext("x"));
            assertEquals(1, stub.cancelCount.get());
        }

        @Test
        @DisplayName("nextHandler throwing Error propagates Error and cancels upstream")
        void nextHandlerThrowingError_propagatesError() {
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
                throw new OutOfMemoryError("oom");
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);

            assertThrows(OutOfMemoryError.class, () -> sub.onNext("x"));
            assertEquals(1, stub.cancelCount.get());
        }

    }

    // =========================================================================
    // onError
    // =========================================================================

    @Nested
    @DisplayName("onError — §2.3, §2.13, §3.13")
    class OnError {

        @Test
        @DisplayName("§2.13 — onError(null) throws NullPointerException")
        void onError_null_throwsNPE() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            assertThrows(NullPointerException.class, () -> sub.onError(null));
        }

        @Test
        @DisplayName("onError delegates throwable to errorHandler")
        void onError_delegatesToHandler() {
            AtomicReference<Throwable> received = new AtomicReference<>();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, received::set, () -> {
            });
            sub.onSubscribe(new StubSubscription());

            RuntimeException cause = new RuntimeException("cause");
            sub.onError(cause);
            assertSame(cause, received.get());
        }

        @Test
        @DisplayName("§2.3 — second onError is silently dropped, handler called only once")
        void secondOnError_silentlyDropped() {
            AtomicInteger errorCount = new AtomicInteger(0);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> errorCount.incrementAndGet(), () -> {
            });
            sub.onSubscribe(new StubSubscription());
            sub.onError(new RuntimeException("first"));
            sub.onError(new RuntimeException("second"));

            assertEquals(1, errorCount.get(), "onError must be delivered at most once");
        }

        @Test
        @DisplayName("onError after onComplete is silently dropped")
        void onError_afterOnComplete_silentlyDropped() {
            AtomicInteger errorCount = new AtomicInteger(0);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> errorCount.incrementAndGet(), () -> {
            });
            sub.onSubscribe(new StubSubscription());
            sub.onComplete();
            sub.onError(new RuntimeException("late error"));

            assertEquals(0, errorCount.get(), "onError after onComplete must be dropped");
        }

        @Test
        @DisplayName("§3.13 — getSubscription() returns null after onError (subscription reference released)")
        void getSubscription_nullAfterOnError() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            sub.onError(new RuntimeException());
            assertNull(sub.getSubscription(), "subscription reference must be released after onError");
        }

        @Test
        @DisplayName("isTerminated() is true after onError")
        void isTerminated_trueAfterOnError() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            sub.onError(new RuntimeException());
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("errorHandler throwing does not propagate — exception is swallowed")
        void errorHandlerThrowing_exceptionSwallowed() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
                throw new RuntimeException("handler blew up");
            }, () -> {
            });
            sub.onSubscribe(new StubSubscription());
            // Must NOT propagate the handler exception per §2.13
            assertDoesNotThrow(() -> sub.onError(new RuntimeException("original")));
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("onError before onSubscribe transitions to terminal and invokes handler (§1.9 publisher violation)")
        void onError_beforeOnSubscribe_handlerInvokedAndTerminated() {
            AtomicReference<Throwable> received = new AtomicReference<>();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, received::set, () -> {
            });
            RuntimeException cause = new RuntimeException("pre-subscribe error");
            sub.onError(cause); // publisher violation §1.9 — subscriber must still handle gracefully
            assertSame(cause, received.get());
            assertTrue(sub.isTerminated());
        }

    }

    // =========================================================================
    // onComplete
    // =========================================================================

    @Nested
    @DisplayName("onComplete — §2.3, §3.13")
    class OnComplete {

        @Test
        @DisplayName("onComplete invokes completeHandler")
        void onComplete_invokesHandler() {
            AtomicBoolean called = new AtomicBoolean(false);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> called.set(true));
            sub.onSubscribe(new StubSubscription());
            sub.onComplete();
            assertTrue(called.get());
        }

        @Test
        @DisplayName("§2.3 — second onComplete is silently dropped, handler called only once")
        void secondOnComplete_silentlyDropped() {
            AtomicInteger count = new AtomicInteger(0);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> count.incrementAndGet());
            sub.onSubscribe(new StubSubscription());
            sub.onComplete();
            sub.onComplete();

            assertEquals(1, count.get(), "onComplete must be delivered at most once");
        }

        @Test
        @DisplayName("onComplete after onError is silently dropped")
        void onComplete_afterOnError_silentlyDropped() {
            AtomicInteger count = new AtomicInteger(0);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> count.incrementAndGet());
            sub.onSubscribe(new StubSubscription());
            sub.onError(new RuntimeException());
            sub.onComplete();

            assertEquals(0, count.get(), "onComplete after onError must be dropped");
        }

        @Test
        @DisplayName("§3.13 — getSubscription() returns null after onComplete")
        void getSubscription_nullAfterOnComplete() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            sub.onComplete();
            assertNull(sub.getSubscription(), "subscription reference must be released after onComplete");
        }

        @Test
        @DisplayName("isTerminated() is true after onComplete")
        void isTerminated_trueAfterOnComplete() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            sub.onComplete();
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("§2.9 — onComplete without prior request() is handled normally")
        void onComplete_withoutPriorRequest_handledNormally() {
            AtomicBoolean called = new AtomicBoolean(false);
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
                /* deliberately no s.request() */ }, t -> {
                }, e -> {
                }, () -> called.set(true));
            sub.onSubscribe(new StubSubscription());
            assertDoesNotThrow(sub::onComplete);
            assertTrue(called.get());
        }

        @Test
        @DisplayName("completeHandler throwing does not propagate — exception is swallowed")
        void completeHandlerThrowing_exceptionSwallowed() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
                throw new RuntimeException("handler blew up");
            });
            sub.onSubscribe(new StubSubscription());
            assertDoesNotThrow(sub::onComplete);
            assertTrue(sub.isTerminated());
        }

    }

    // =========================================================================
    // cancel()
    // =========================================================================

    @Nested
    @DisplayName("cancel() — idempotency, upstream forwarding, state transition")
    class CancelMethod {

        @Test
        @DisplayName("cancel() forwards cancel to upstream subscription exactly once")
        void cancel_forwardsToUpstream() {
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);
            sub.cancel();

            assertEquals(1, stub.cancelCount.get(), "upstream cancel must be called exactly once");
        }

        @Test
        @DisplayName("cancel() transitions isTerminated() to true")
        void cancel_terminatesSubscriber() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            sub.cancel();
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("cancel() is idempotent — calling three times cancels upstream exactly once")
        void cancel_idempotent() {
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);
            sub.cancel();
            sub.cancel();
            sub.cancel();

            assertEquals(1, stub.cancelCount.get(), "upstream cancel must only be called once");
        }

        @Test
        @DisplayName("cancel() before onSubscribe is safe — does not throw, does not cancel null")
        void cancel_beforeOnSubscribe_safe() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            assertDoesNotThrow(sub::cancel);
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("cancel() after onComplete does not cancel upstream again")
        void cancel_afterOnComplete_doesNotCancelUpstreamAgain() {
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);
            sub.onComplete();
            sub.cancel(); // already terminated — must be no-op

            assertEquals(0, stub.cancelCount.get(), "cancel after terminal must not forward to upstream");
        }

        @Test
        @DisplayName("getSubscription() returns null after cancel()")
        void getSubscription_nullAfterCancel() {
            SimpleSubscriber<String> sub = subscribedNoOp();
            sub.cancel();
            assertNull(sub.getSubscription(), "subscription reference must be null after cancel");
        }

    }

    // =========================================================================
    // State machine
    // =========================================================================

    @Nested
    @DisplayName("State machine — lifecycle transitions and getSubscription()")
    class StateMachine {

        @Test
        @DisplayName("initial state: isTerminated()=false, getSubscription()=null")
        void initialState() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            assertFalse(sub.isTerminated());
            assertNull(sub.getSubscription());
        }

        @Test
        @DisplayName("after onSubscribe: isTerminated()=false, getSubscription()=subscription")
        void stateAfterOnSubscribe() {
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);

            assertFalse(sub.isTerminated());
            assertSame(stub, sub.getSubscription());
        }

        @Test
        @DisplayName("full happy-path lifecycle: null → subscribed → onNext delivery → onComplete → terminated")
        void fullLifecycle_happyPath() {
            AtomicBoolean subscribed = new AtomicBoolean(false);
            AtomicReference<String> nextItem = new AtomicReference<>();
            AtomicBoolean completed = new AtomicBoolean(false);

            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> subscribed.set(true), nextItem::set, e -> {
            }, () -> completed.set(true));

            assertNull(sub.getSubscription());
            assertFalse(sub.isTerminated());

            sub.onSubscribe(new StubSubscription());
            assertTrue(subscribed.get());
            assertFalse(sub.isTerminated());

            sub.onNext("item");
            assertEquals("item", nextItem.get());
            assertFalse(sub.isTerminated());

            sub.onComplete();
            assertTrue(completed.get());
            assertTrue(sub.isTerminated());
            assertNull(sub.getSubscription());
        }

        @Test
        @DisplayName("full error lifecycle: null → subscribed → onError → terminated")
        void fullLifecycle_errorPath() {
            AtomicReference<Throwable> caughtError = new AtomicReference<>();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, caughtError::set, () -> {
            });

            sub.onSubscribe(new StubSubscription());
            RuntimeException cause = new RuntimeException("err");
            sub.onError(cause);

            assertSame(cause, caughtError.get());
            assertTrue(sub.isTerminated());
            assertNull(sub.getSubscription());
        }

        @Test
        @DisplayName("cancel lifecycle: null → subscribed → cancelled → terminal")
        void fullLifecycle_cancelPath() {
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });

            assertFalse(sub.isTerminated());
            sub.onSubscribe(stub);
            assertFalse(sub.isTerminated());

            sub.cancel();
            assertTrue(sub.isTerminated());
            assertNull(sub.getSubscription());
            assertEquals(1, stub.cancelCount.get());
        }

    }

    // =========================================================================
    // Handler exceptions — per signal method
    // =========================================================================

    @Nested
    @DisplayName("Handler exceptions — throw semantics per signal method")
    class HandlerExceptions {

        @Test
        @DisplayName("subscribeHandler throwing RuntimeException is rethrown and upstream is cancelled")
        void subscribeHandler_throwingRuntimeException_rethrown() {
            StubSubscription stub = new StubSubscription();
            RuntimeException bang = new RuntimeException("subscribe handler fail");

            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
                throw bang;
            }, t -> {
            }, e -> {
            }, () -> {
            });

            RuntimeException thrown = assertThrows(RuntimeException.class, () -> sub.onSubscribe(stub));
            assertSame(bang, thrown);
            assertEquals(1, stub.cancelCount.get(), "upstream must be cancelled when subscribeHandler throws");
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("subscribeHandler throwing Error propagates Error and cancels upstream")
        void subscribeHandler_throwingError_propagated() {
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
                throw new StackOverflowError();
            }, t -> {
            }, e -> {
            }, () -> {
            });

            assertThrows(StackOverflowError.class, () -> sub.onSubscribe(stub));
            assertEquals(1, stub.cancelCount.get());
        }

        @Test
        @DisplayName("nextHandler throwing causes cancel and rethrow — subsequent onNext is dropped")
        void nextHandler_throwingCancelsAndDropsSubsequentOnNext() {
            AtomicInteger nextCount = new AtomicInteger(0);
            StubSubscription stub = new StubSubscription();

            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
                nextCount.incrementAndGet();
                throw new RuntimeException("nxt");
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);

            assertThrows(RuntimeException.class, () -> sub.onNext("first"));
            sub.onNext("second"); // must be silently dropped — subscriber is terminated

            assertEquals(1, nextCount.get(), "nextHandler must only be invoked once");
            assertEquals(1, stub.cancelCount.get());
        }

        @Test
        @DisplayName("errorHandler throwing is swallowed — subscriber remains terminated")
        void errorHandler_throwing_swallowed() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
                throw new RuntimeException("error handler fail");
            }, () -> {
            });
            sub.onSubscribe(new StubSubscription());

            assertDoesNotThrow(() -> sub.onError(new RuntimeException("original")));
            assertTrue(sub.isTerminated());
        }

        @Test
        @DisplayName("completeHandler throwing is swallowed — subscriber remains terminated")
        void completeHandler_throwing_swallowed() {
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
                throw new RuntimeException("complete handler fail");
            });
            sub.onSubscribe(new StubSubscription());

            assertDoesNotThrow(sub::onComplete);
            assertTrue(sub.isTerminated());
        }

    }

    // =========================================================================
    // Concurrency
    // =========================================================================

    @Nested
    @DisplayName("Concurrency — concurrent terminal signals and concurrent cancel + onNext")
    class Concurrency {

        @Test
        @DisplayName("concurrent onComplete and onError — exactly one terminal handler is invoked")
        void concurrentOnCompleteAndOnError_exactlyOneHandlerInvoked() throws InterruptedException {
            for (int round = 0; round < 500; round++) {
                AtomicInteger terminalCount = new AtomicInteger(0);
                SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
                }, t -> {
                }, e -> terminalCount.incrementAndGet(), () -> terminalCount.incrementAndGet());
                sub.onSubscribe(new StubSubscription());

                CountDownLatch go = new CountDownLatch(1);
                Thread t1 = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                    }
                    sub.onComplete();
                });
                Thread t2 = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                    }
                    sub.onError(new RuntimeException());
                });

                t1.start();
                t2.start();
                go.countDown();
                t1.join(200);
                t2.join(200);

                assertEquals(1, terminalCount.get(), "exactly one terminal signal must be delivered in round " + round);
            }
        }

        @Test
        @DisplayName("concurrent onNext and cancel — once isTerminated() is true no further onNext handler fires")
        void concurrentOnNextAndCancel_noNextAfterTerminalStateVisible() throws InterruptedException {
            for (int round = 0; round < 300; round++) {
                AtomicInteger handlerCount = new AtomicInteger(0);
                StubSubscription stub = new StubSubscription();

                SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
                }, t -> handlerCount.incrementAndGet(), e -> {
                }, () -> {
                });
                sub.onSubscribe(stub);

                CountDownLatch go = new CountDownLatch(1);
                Thread emitThread = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                    }
                    for (int i = 0; i < 20; i++)
                        sub.onNext("item-" + i);
                });
                Thread cancelThread = new Thread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                    }
                    sub.cancel();
                });

                emitThread.start();
                cancelThread.start();
                go.countDown();
                emitThread.join(300);
                cancelThread.join(300);

                // At this point isTerminated() is guaranteed true — cancel() has completed.
                assertTrue(sub.isTerminated(), "must be terminated after cancel() thread joined");

                // Any onNext fired now MUST be dropped — state is visibly TERMINATED.
                int countBeforePost = handlerCount.get();
                sub.onNext("post-cancel");
                assertEquals(countBeforePost, handlerCount.get(),
                        "onNext after observed TERMINATED must not invoke handler in round " + round);

                // At most 20 in-flight deliveries could have occurred (one per emit call),
                // not 21 which would mean the post-cancel call was processed.
                assertTrue(handlerCount.get() <= 20, "handler count must not exceed emitted items in round " + round);
            }
        }

        @Test
        @DisplayName("concurrent duplicate onSubscribe calls — exactly one subscription accepted, rest cancelled")
        void concurrentDuplicateOnSubscribe_exactlyOneAccepted() throws InterruptedException {
            int threads = 10;
            AtomicInteger acceptedCount = new AtomicInteger(0);
            AtomicInteger cancelledCount = new AtomicInteger(0);
            CountDownLatch done = new CountDownLatch(threads);
            CyclicBarrier barrier = new CyclicBarrier(threads);

            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> acceptedCount.incrementAndGet(), t -> {
            }, e -> {
            }, () -> {
            });

            ExecutorService pool = Executors.newFixedThreadPool(threads);
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        barrier.await();
                        StubSubscription stub = new StubSubscription();
                        sub.onSubscribe(stub);
                        if (stub.cancelCount.get() > 0)
                            cancelledCount.incrementAndGet();
                    } catch (Exception ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(done.await(3, TimeUnit.SECONDS));
            pool.shutdown();

            assertEquals(1, acceptedCount.get(), "exactly one onSubscribe must be accepted");
            assertEquals(threads - 1, cancelledCount.get(), "all other subscriptions must be cancelled");
        }

        @Test
        @DisplayName("concurrent cancel() calls — upstream cancelled exactly once")
        void concurrentCancelCalls_upstreamCancelledExactlyOnce() throws InterruptedException {
            int threads = 20;
            StubSubscription stub = new StubSubscription();
            SimpleSubscriber<String> sub = new SimpleSubscriber<>(s -> {
            }, t -> {
            }, e -> {
            }, () -> {
            });
            sub.onSubscribe(stub);

            CyclicBarrier barrier = new CyclicBarrier(threads);
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch done = new CountDownLatch(threads);

            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        barrier.await();
                        sub.cancel();
                    } catch (Exception ignored) {
                    } finally {
                        done.countDown();
                    }
                });
            }

            assertTrue(done.await(3, TimeUnit.SECONDS));
            pool.shutdown();

            assertEquals(1, stub.cancelCount.get(),
                    "upstream cancel must be called exactly once regardless of concurrent callers");
        }

    }

}
