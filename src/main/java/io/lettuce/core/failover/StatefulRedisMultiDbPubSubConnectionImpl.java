package io.lettuce.core.failover;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.lettuce.core.RedisDatabase;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.failover.api.StatefulRedisMultiDbPubSubConnection;
import io.lettuce.core.json.JsonParser;
import io.lettuce.core.pubsub.RedisPubSubAsyncCommandsImpl;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.RedisPubSubReactiveCommandsImpl;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnectionImpl;
import io.lettuce.core.pubsub.api.async.RedisPubSubAsyncCommands;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.lettuce.core.resource.ClientResources;

public class StatefulRedisMultiDbPubSubConnectionImpl<K, V> extends StatefulRedisMultiDbConnectionImpl<K, V>
        implements StatefulRedisMultiDbPubSubConnection<K, V> {

    private final Set<RedisPubSubListener> pubSubListeners = ConcurrentHashMap.newKeySet();

    public StatefulRedisMultiDbPubSubConnectionImpl(Map<RedisURI, RedisDatabase<K, V>> connections, ClientResources resources,
            RedisCodec<K, V> codec, Supplier<JsonParser> parser) {
        super(connections, resources, codec, parser);
    }

    @Override
    public void addListener(RedisPubSubListener<K, V> listener) {
        pubSubListeners.add(listener);
        ((StatefulRedisPubSubConnectionImpl) (current.getConnection())).addListener(listener);
    }

    @Override
    public void removeListener(RedisPubSubListener<K, V> listener) {
        pubSubListeners.remove(listener);
        ((StatefulRedisPubSubConnectionImpl) (current.getConnection())).removeListener(listener);
    }

    @Override
    public RedisPubSubAsyncCommands<K, V> async() {
        return (RedisPubSubAsyncCommands<K, V>) async;
    }

    @Override
    protected RedisPubSubAsyncCommandsImpl<K, V> newRedisAsyncCommandsImpl() {
        return new RedisPubSubAsyncCommandsImpl<>(this, codec);
    }

    @Override
    public RedisPubSubCommands<K, V> sync() {
        return (RedisPubSubCommands<K, V>) sync;
    }

    @Override
    protected RedisPubSubCommands<K, V> newRedisSyncCommandsImpl() {
        return syncHandler(async(), RedisPubSubCommands.class);
    }

    @Override
    public RedisPubSubReactiveCommands<K, V> reactive() {
        return (RedisPubSubReactiveCommands<K, V>) reactive;
    }

    @Override
    protected RedisPubSubReactiveCommandsImpl<K, V> newRedisReactiveCommandsImpl() {
        return new RedisPubSubReactiveCommandsImpl<>(this, codec);
    }

    @Override
    public void switchToDatabase(RedisURI redisURI) {

        RedisDatabase<K, V> temp = current;
        super.switchToDatabase(redisURI);
        pubSubListeners.forEach(listener -> {
            ((StatefulRedisPubSubConnectionImpl) (current.getConnection())).addListener(listener);
            ((StatefulRedisPubSubConnectionImpl) (temp.getConnection())).removeListener(listener);
        });
    }

}
