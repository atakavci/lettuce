package io.lettuce.core.failover.api;

import io.lettuce.core.RedisURI;
import io.lettuce.core.failover.CircuitBreaker;

public interface BaseRedisMultiDb {

    void switchToDatabase(RedisURI redisURI);

    RedisURI getCurrentEndpoint();

    Iterable<RedisURI> getEndpoints();

    /**
     * Get the circuit breaker for a specific endpoint.
     *
     * @param endpoint the Redis endpoint URI
     * @return the circuit breaker for the endpoint
     * @throws IllegalArgumentException if the endpoint is not known
     */
    CircuitBreaker getCircuitBreaker(RedisURI endpoint);

}
