package io.lettuce.core;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.failover.ManagedCommandQueue;

/**
 * Represents a Redis database with a weight and a connection.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public class RedisDatabase<K, V> {

    private final float weight;

    private final StatefulRedisConnection<K, V> connection;

    private final RedisURI redisURI;

    private final ManagedCommandQueue managedCommandQueue;

    public RedisDatabase(RedisURI redisURI, float weight, StatefulRedisConnection<K, V> connection, ManagedCommandQueue managedCommandQueue) {
        this.redisURI = redisURI;
        this.weight = weight;
        this.connection = connection;
        this.managedCommandQueue = managedCommandQueue;
    }

    public float getWeight() {
        return weight;
    }

    public StatefulRedisConnection<K, V> getConnection() {
        return connection;
    }

    public RedisURI getRedisURI() {
        return redisURI;
    }

    public ManagedCommandQueue getCommandQueue() {
        return managedCommandQueue;
    }

}
