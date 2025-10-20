package io.lettuce.core;

import io.lettuce.core.api.StatefulRedisConnection;

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

    public RedisDatabase(RedisURI redisURI, float weight, StatefulRedisConnection<K, V> connection) {
        this.redisURI = redisURI;
        this.weight = weight;
        this.connection = connection;
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

}
