package io.lettuce.core.failover.api;

import io.lettuce.core.RedisURI;
import io.lettuce.core.failover.CircuitBreaker;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

/**
 * Stateful multi-database Redis PubSub connection that supports failover between multiple endpoints. Each endpoint has its own
 * circuit breaker for tracking command metrics.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface StatefulRedisMultiDbPubSubConnection<K, V> extends StatefulRedisPubSubConnection<K, V>, BaseRedisMultiDb {

    /**
     * Get the circuit breaker for a specific endpoint.
     *
     * @param endpoint the Redis endpoint URI
     * @return the circuit breaker for the endpoint
     * @throws IllegalArgumentException if the endpoint is not known
     */
    CircuitBreaker getCircuitBreaker(RedisURI endpoint);

}
