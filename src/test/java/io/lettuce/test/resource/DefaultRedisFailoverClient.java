package io.lettuce.test.resource;

import java.util.List;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.failover.RedisFailoverClient;
import io.lettuce.test.settings.TestSettings;

/**
 * @author Mark Paluch
 * @author Hari Mani
 */
public class DefaultRedisFailoverClient {

    private static final DefaultRedisFailoverClient instance = new DefaultRedisFailoverClient();

    private final RedisFailoverClient redisClient;

    private DefaultRedisFailoverClient() {
        redisClient = RedisFailoverClient.create(getEndpoints());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> FastShutdown.shutdown(redisClient)));
    }

    /**
     * Do not close the client.
     *
     * @return the default redis client for the tests.
     */
    public static RedisFailoverClient get() {
        return instance.redisClient;
    }

    private List<RedisURI> getEndpoints() {
        return java.util.Arrays.asList(RedisURI.create(TestSettings.host(), TestSettings.port()),
                RedisURI.create(TestSettings.host(), TestSettings.port(1)));
    }

}
