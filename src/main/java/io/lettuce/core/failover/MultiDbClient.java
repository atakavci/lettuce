package io.lettuce.core.failover;

import java.util.Collection;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import io.lettuce.core.failover.api.StatefulRedisMultiDbPubSubConnection;
import io.lettuce.core.resource.ClientResources;

public interface MultiDbClient extends BaseClient {

    public static MultiDbClient create() {
        return new MultiDbClientImpl(null, null);
    }

    public static MultiDbClient create(Collection<DatabaseConfig> databaseConfigs) {
        return create(null, databaseConfigs);
    }

    public static MultiDbClient create(ClientResources resources, Collection<DatabaseConfig> databaseConfigs) {
        if (resources == null) {
            throw new IllegalArgumentException("resources must not be null");
        }
        if (databaseConfigs == null || databaseConfigs.isEmpty()) {
            throw new IllegalArgumentException("databaseConfigs must not be empty");
        }
        return new MultiDbClientImpl(resources, databaseConfigs);
    }

    Collection<RedisURI> getRedisURIs();

    <K, V> StatefulRedisMultiDbConnection<K, V> connect(RedisCodec<K, V> codec);

    StatefulRedisMultiDbConnection<String, String> connect();

    <K, V> StatefulRedisMultiDbPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec);

    StatefulRedisMultiDbPubSubConnection<String, String> connectPubSub();

}
