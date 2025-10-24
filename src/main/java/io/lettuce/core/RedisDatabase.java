package io.lettuce.core;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.failover.ManagedCommandQueue;

/**
 * Represents a Redis database with a weight and a connection.
 *
 * @param <C> Connection type.
 */
public class RedisDatabase<C extends StatefulRedisConnection<?, ?>> {

    private final float weight;

    private final C connection;

    private final RedisURI redisURI;

    private final ManagedCommandQueue managedCommandQueue;

    public RedisDatabase(RedisURI redisURI, float weight, C connection, ManagedCommandQueue managedCommandQueue) {
        this.redisURI = redisURI;
        this.weight = weight;
        this.connection = connection;
        this.managedCommandQueue = managedCommandQueue;
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

    public ManagedCommandQueue getCommandQueue() {
        return managedCommandQueue;
    }

}
