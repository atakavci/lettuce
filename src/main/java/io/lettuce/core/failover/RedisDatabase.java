package io.lettuce.core.failover;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * Represents a Redis database with a weight and a connection.
 *
 * @param <C> Connection type.
 */
public class RedisDatabase<C extends StatefulRedisConnection<?, ?>> {

    private final float weight;

    private final C connection;

    private final RedisURI redisURI;

    private final DatabaseEndpoint databaseEndpoint;

    private final CircuitBreaker circuitBreaker;

    public RedisDatabase(RedisURI redisURI, float weight, C connection, DatabaseEndpoint databaseEndpoint) {
        this.redisURI = redisURI;
        this.weight = weight;
        this.connection = connection;
        this.databaseEndpoint = databaseEndpoint;
        this.circuitBreaker = new CircuitBreaker();
        databaseEndpoint.setCircuitBreaker(circuitBreaker);
    }

    public float getWeight() {
        return weight;
    }

    public C getConnection() {
        return connection;
    }

    public RedisURI getRedisURI() {
        return redisURI;
    }

    public DatabaseEndpoint getDatabaseEndpoint() {
        return databaseEndpoint;
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

}
