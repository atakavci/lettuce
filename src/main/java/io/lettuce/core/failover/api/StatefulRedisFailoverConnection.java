package io.lettuce.core.failover.api;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

public interface StatefulRedisFailoverConnection<K, V> extends StatefulRedisConnection<K, V> {

    void switchToDatabase(RedisURI redisURI);

    RedisURI getCurrentEndpoint();

    Iterable<RedisURI> getEndpoints();

}
