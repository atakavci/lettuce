package io.lettuce.core.failover;

/**
 * Circuit breaker for tracking command metrics and managing circuit breaker state. Wraps CircuitBreakerMetrics and exposes it
 * via {@link #getMetrics()}.
 *
 * @author Augment
 */
public class CircuitBreaker {

    private final CircuitBreakerMetrics metrics;

    /**
     * Create a circuit breaker instance.
     */
    public CircuitBreaker() {
        this.metrics = new CircuitBreakerMetricsImpl();
    }

    /**
     * Get the metrics tracked by this circuit breaker.
     *
     * @return the circuit breaker metrics
     */
    public CircuitBreakerMetrics getMetrics() {
        return metrics;
    }

    @Override
    public String toString() {
        return "CircuitBreaker{" + metrics + '}';
    }

}
