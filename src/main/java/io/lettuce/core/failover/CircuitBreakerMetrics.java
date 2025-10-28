package io.lettuce.core.failover;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics for circuit breaker tracking successes and failures. Thread-safe using atomic counters.
 *
 * @author Augment
 */
public class CircuitBreakerMetrics {

    private final AtomicLong successCount = new AtomicLong();

    private final AtomicLong failureCount = new AtomicLong();

    /**
     * Create metrics instance.
     */
    public CircuitBreakerMetrics() {
    }

    /**
     * Record a successful command execution.
     */
    public void recordSuccess() {
        successCount.incrementAndGet();
    }

    /**
     * Record a failed command execution.
     *
     * @param error the error that occurred
     */
    public void recordFailure(Throwable error) {
        failureCount.incrementAndGet();
    }

    /**
     * Get the total number of successful commands.
     *
     * @return success count
     */
    public long getSuccessCount() {
        return successCount.get();
    }

    /**
     * Get the total number of failed commands.
     *
     * @return failure count
     */
    public long getFailureCount() {
        return failureCount.get();
    }

    /**
     * Reset all metrics to zero.
     */
    public void reset() {
        successCount.set(0);
        failureCount.set(0);
    }

    @Override
    public String toString() {
        return "CircuitBreakerMetrics{" + "successes=" + successCount.get() + ", failures=" + failureCount.get() + '}';
    }

}
