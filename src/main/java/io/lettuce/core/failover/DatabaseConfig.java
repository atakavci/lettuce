package io.lettuce.core.failover;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.internal.LettuceAssert;

/**
 * Configuration for a database in a multi-database client. Holds the Redis URI, weight for load balancing, and client options.
 *
 * @author Augment
 */
public class DatabaseConfig {

    private final RedisURI redisURI;

    private final float weight;

    private final ClientOptions clientOptions;

    /**
     * Create a new database configuration.
     *
     * @param redisURI the Redis URI, must not be {@code null}
     * @param weight the weight for load balancing, must be greater than 0
     * @param clientOptions the client options, can be {@code null} to use defaults
     */
    public DatabaseConfig(RedisURI redisURI, float weight, ClientOptions clientOptions) {
        LettuceAssert.notNull(redisURI, "RedisURI must not be null");
        LettuceAssert.isTrue(weight > 0, "Weight must be greater than 0");

        this.redisURI = redisURI;
        this.weight = weight;
        this.clientOptions = clientOptions;
    }

    /**
     * Create a new database configuration with default client options.
     *
     * @param redisURI the Redis URI, must not be {@code null}
     * @param weight the weight for load balancing, must be greater than 0
     */
    public DatabaseConfig(RedisURI redisURI, float weight) {
        this(redisURI, weight, null);
    }

    /**
     * Get the Redis URI.
     *
     * @return the Redis URI
     */
    public RedisURI getRedisURI() {
        return redisURI;
    }

    /**
     * Get the weight for load balancing.
     *
     * @return the weight
     */
    public float getWeight() {
        return weight;
    }

    /**
     * Get the client options.
     *
     * @return the client options, can be {@code null}
     */
    public ClientOptions getClientOptions() {
        return clientOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DatabaseConfig))
            return false;

        DatabaseConfig that = (DatabaseConfig) o;

        if (Float.compare(that.weight, weight) != 0)
            return false;
        if (!redisURI.equals(that.redisURI))
            return false;
        return clientOptions != null ? clientOptions.equals(that.clientOptions) : that.clientOptions == null;
    }

    @Override
    public int hashCode() {
        int result = redisURI.hashCode();
        result = 31 * result + (weight != +0.0f ? Float.floatToIntBits(weight) : 0);
        result = 31 * result + (clientOptions != null ? clientOptions.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "DatabaseConfig{" + "redisURI=" + redisURI + ", weight=" + weight + ", clientOptions=" + clientOptions + '}';
    }

}
