package io.lettuce.core.failover;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisDatabase;
import io.lettuce.core.RedisFuture;
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
import io.netty.util.internal.logging.InternalLoggerFactory;

public class StatefulRedisMultiDbPubSubConnectionImpl<C extends StatefulRedisPubSubConnectionImpl<K, V>, K, V>
        extends StatefulRedisMultiDbConnectionImpl<C, K, V> implements StatefulRedisMultiDbPubSubConnection<K, V> {

    private final Set<RedisPubSubListener<K, V>> pubSubListeners = ConcurrentHashMap.newKeySet();

    public StatefulRedisMultiDbPubSubConnectionImpl(Map<RedisURI, RedisDatabase<C>> connections, ClientResources resources,
            RedisCodec<K, V> codec, Supplier<JsonParser> parser) {
        super(connections, resources, codec, parser);
    }

    @Override
    public void addListener(RedisPubSubListener<K, V> listener) {
        pubSubListeners.add(listener);
        StatefulRedisPubSubConnectionImpl<K, V> pubSubConnection = (StatefulRedisPubSubConnectionImpl<K, V>) (current
                .getConnection());
        pubSubConnection.addListener(listener);
    }

    @Override
    public void removeListener(RedisPubSubListener<K, V> listener) {
        pubSubListeners.remove(listener);
        StatefulRedisPubSubConnectionImpl<K, V> pubSubConnection = (StatefulRedisPubSubConnectionImpl<K, V>) (current
                .getConnection());
        pubSubConnection.removeListener(listener);
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

        RedisDatabase<C> fromDb = current;
        super.switchToDatabase(redisURI);
        pubSubListeners.forEach(listener -> {
            ((StatefulRedisPubSubConnectionImpl<K, V>) (current.getConnection())).addListener(listener);
            ((StatefulRedisPubSubConnectionImpl<K, V>) (fromDb.getConnection())).removeListener(listener);
        });
        moveSubscriptions(fromDb, current);
    }

    public void moveSubscriptions(RedisDatabase<?> fromDb, RedisDatabase<?> toDb) {

        AdvancedPubSubEndpoint<K, V> fromEndpoint = (AdvancedPubSubEndpoint<K, V>) fromDb.getCommandQueue();

        List<RedisFuture<Void>> resubscribeList = new ArrayList<>();

        if (fromEndpoint.hasChannelSubscriptions()) {
            resubscribeList.add(async().subscribe(toArray(fromEndpoint.getChannels())));
        }

        if (fromEndpoint.hasShardChannelSubscriptions()) {
            resubscribeList.add(async().ssubscribe(toArray(fromEndpoint.getShardChannels())));
        }

        if (fromEndpoint.hasPatternSubscriptions()) {
            resubscribeList.add(async().psubscribe(toArray(fromEndpoint.getPatterns())));
        }

        for (RedisFuture<Void> command : resubscribeList) {
            command.exceptionally(throwable -> {
                if (throwable instanceof RedisCommandExecutionException) {
                    InternalLoggerFactory.getInstance(getClass()).warn("Re-subscribe failed: " + command.getError());
                }
                return null;
            });
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T[] toArray(Collection<T> c) {
        Class<T> cls = (Class<T>) c.iterator().next().getClass();
        T[] array = (T[]) Array.newInstance(cls, c.size());
        return c.toArray(array);
    }

}
