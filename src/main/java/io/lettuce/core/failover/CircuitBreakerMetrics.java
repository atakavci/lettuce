package io.lettuce.core.failover;

import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.RedisURI;

/**
 * Metrics for circuit breaker tracking command attempts, successes, and failures. Thread-safe using atomic counters.
 *
 * @author Augment
 */
public class CircuitBreakerMetrics {

    private final RedisURI endpoint;

    private final AtomicLong attemptCount = new AtomicLong();

    private final AtomicLong successCount = new AtomicLong();

    private final AtomicLong failureCount = new AtomicLong();

    /**
     * Create metrics for a specific endpoint.
     *
     * @param endpoint the Redis endpoint URI
     */
    public CircuitBreakerMetrics(RedisURI endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Record a command attempt.
     */
    public void recordAttempt() {
        attemptCount.incrementAndGet();
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
     * Get the total number of command attempts.
     *
     * @return attempt count
     */
    public long getAttemptCount() {
        return attemptCount.get();
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
     * Get the endpoint this metrics tracks.
     *
     * @return the Redis endpoint URI
     */
    public RedisURI getEndpoint() {
        return endpoint;
    }

    /**
     * Reset all metrics to zero.
     */
    public void reset() {
        attemptCount.set(0);
        successCount.set(0);
        failureCount.set(0);
    }

    @Override
    public String toString() {
        return "CircuitBreakerMetrics{" + "endpoint=" + endpoint + ", attempts=" + attemptCount.get() + ", successes="
                + successCount.get() + ", failures=" + failureCount.get() + '}';
    }

}
