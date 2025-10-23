package io.lettuce.core.failover;

import java.util.List;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import io.lettuce.core.resource.ClientResources;

public interface MultiDbClient extends BaseClient {

    public static MultiDbClient create() {
        return new MultiDbClientImpl(null, null);
    }

    public static MultiDbClient create(List<RedisURI> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }
        return new MultiDbClientImpl(null, endpoints);
    }

    public static MultiDbClient create(ClientResources resources, List<RedisURI> endpoints) {
        if (resources == null) {
            throw new IllegalArgumentException("resources must not be null");
        }
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }
        return new MultiDbClientImpl(resources, endpoints);
    }

    <K, V> StatefulRedisMultiDbConnection<K, V> connect(RedisCodec<K, V> codec);

    StatefulRedisMultiDbConnection<String, String> connect();

}
