package io.lettuce.core.failover.api;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.failover.CircuitBreaker;

/**
 * Stateful multi-database Redis connection that supports failover between multiple endpoints. Each endpoint has its own circuit
 * breaker for tracking command metrics.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface StatefulRedisMultiDbConnection<K, V> extends StatefulRedisConnection<K, V>, BaseRedisMultiDb {

    /**
     * Get the circuit breaker for a specific endpoint.
     *
     * @param endpoint the Redis endpoint URI
     * @return the circuit breaker for the endpoint
     * @throws IllegalArgumentException if the endpoint is not known
     */
    CircuitBreaker getCircuitBreaker(RedisURI endpoint);

}
