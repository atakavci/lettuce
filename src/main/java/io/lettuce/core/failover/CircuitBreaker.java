package io.lettuce.core.failover;

/**
 * Circuit breaker for tracking command metrics and managing circuit breaker state. Wraps CircuitBreakerMetrics and provides
 * methods to record command outcomes.
 *
 * @author Augment
 */
public class CircuitBreaker {

    private final CircuitBreakerMetrics metrics;

    /**
     * Create a circuit breaker instance.
     */
    public CircuitBreaker() {
        this.metrics = new CircuitBreakerMetrics();
    }

    /**
     * Get the metrics tracked by this circuit breaker.
     *
     * @return the circuit breaker metrics
     */
    public CircuitBreakerMetrics getMetrics() {
        return metrics;
    }

    /**
     * Record a successful command execution.
     */
    public void recordSuccess() {
        metrics.recordSuccess();
    }

    /**
     * Record a failed command execution.
     *
     * @param error the error that occurred
     */
    public void recordFailure(Throwable error) {
        metrics.recordFailure(error);
    }

    /**
     * Get the total number of successful commands.
     *
     * @return success count
     */
    public long getSuccessCount() {
        return metrics.getSuccessCount();
    }

    /**
     * Get the total number of failed commands.
     *
     * @return failure count
     */
    public long getFailureCount() {
        return metrics.getFailureCount();
    }

    /**
     * Reset all metrics to zero.
     */
    public void reset() {
        metrics.reset();
    }

    @Override
    public String toString() {
        return "CircuitBreaker{" + metrics + '}';
    }

}
