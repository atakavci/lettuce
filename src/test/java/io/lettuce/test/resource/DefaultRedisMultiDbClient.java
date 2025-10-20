package io.lettuce.test.resource;

import java.util.List;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.failover.MultiDbClient;
import io.lettuce.test.settings.TestSettings;

/**
 * @author Mark Paluch
 * @author Hari Mani
 */
public class DefaultRedisMultiDbClient {

    private static final DefaultRedisMultiDbClient instance = new DefaultRedisMultiDbClient();

    private final MultiDbClient redisClient;

    private DefaultRedisMultiDbClient() {
        redisClient = MultiDbClient.create(getEndpoints());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> FastShutdown.shutdown(redisClient)));
    }

    /**
     * Do not close the client.
     *
     * @return the default redis client for the tests.
     */
    public static MultiDbClient get() {
        return instance.redisClient;
    }

    private List<RedisURI> getEndpoints() {
        return java.util.Arrays.asList(RedisURI.create(TestSettings.host(), TestSettings.port()),
                RedisURI.create(TestSettings.host(), TestSettings.port(1)));
    }

}
