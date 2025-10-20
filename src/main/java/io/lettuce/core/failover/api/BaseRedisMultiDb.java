package io.lettuce.core.failover.api;

import io.lettuce.core.RedisURI;

public interface BaseRedisMultiDb {

    void switchToDatabase(RedisURI redisURI);

    RedisURI getCurrentEndpoint();

    Iterable<RedisURI> getEndpoints();

}
