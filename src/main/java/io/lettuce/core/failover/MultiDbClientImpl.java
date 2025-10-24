package io.lettuce.core.failover;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.lettuce.core.CommandListenerWriter;
import io.lettuce.core.ConnectionFuture;
import io.lettuce.core.Delegating;
import io.lettuce.core.RedisChannelWriter;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisDatabase;
import io.lettuce.core.RedisURI;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import io.lettuce.core.failover.api.StatefulRedisMultiDbPubSubConnection;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.CommandExpiryWriter;
import io.lettuce.core.protocol.CommandHandler;
import io.lettuce.core.protocol.DefaultEndpoint;
import io.lettuce.core.pubsub.PubSubEndpoint;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnectionImpl;
import io.lettuce.core.resource.ClientResources;

/**
 * Failover-aware client that composes multiple standalone Redis endpoints and returns a single Stateful connection wrapper
 * which can switch the active endpoint without requiring users to recreate command objects.
 *
 * Standalone-only POC. Not for Sentinel/Cluster.
 */
class MultiDbClientImpl extends RedisClient implements MultiDbClient {

    private final List<RedisURI> endpoints;

    // private final RedisClient delegate;

    MultiDbClientImpl(ClientResources clientResources, List<RedisURI> endpoints) {
        super(clientResources, new RedisURI());

        // delegate = RedisClient.create(this.getResources());

        this.endpoints = (endpoints == null) ? new ArrayList<>() : new ArrayList<>(endpoints);
        this.endpoints.forEach(uri -> LettuceAssert.notNull(uri, "RedisURI must not be null"));
    }

    /**
     * Open a new connection to a Redis server that treats keys and values as UTF-8 strings.
     *
     * @return A new stateful Redis connection
     */
    public StatefulRedisMultiDbConnection<String, String> connect() {
        return connect(newStringStringCodec());
    }

    // public StatefulRedisMultiDbConnection<String, String> connect(RedisURI redisURI) {
    // endpoints.clear();
    // endpoints.add(redisURI);
    // return connect(newStringStringCodec());
    // }

    public <K, V> StatefulRedisMultiDbConnection<K, V> connect(RedisCodec<K, V> codec) {

        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }

        Map<RedisURI, RedisDatabase<StatefulRedisConnectionImpl<K, V>>> databases = new ConcurrentHashMap<>(endpoints.size());
        for (RedisURI uri : endpoints) {
            // HACK: looks like repeating the implementation all around 'RedisClient.connect' is an overkill.
            // connections.put(uri, connect(codec, uri));
            // Instead we will use it from delegate
            StatefulRedisConnectionImpl<K, V> connection = (StatefulRedisConnectionImpl<K, V>) connect(codec, uri);
            // TODO: 1 / getChannelCount() is a hack. Just introduce the weight parameter properly.
            databases.put(uri, new RedisDatabase<>(uri, 1 / getChannelCount(), connection, getCommandQueue(connection)));
        }

        return new StatefulRedisMultiDbConnectionImpl<StatefulRedisConnectionImpl<K, V>, K, V>(databases, getResources(), codec,
                getOptions().getJsonParser());
    }

    /**
     * Open a new connection to a Redis server that treats keys and values as UTF-8 strings.
     *
     * @return A new stateful Redis connection
     */
    public StatefulRedisMultiDbPubSubConnection<String, String> connectPubSub() {
        return connectPubSub(newStringStringCodec());
    }

    // public StatefulRedisMultiDbPubSubConnection<String, String> connectPubSub(RedisURI redisURI) {
    // endpoints.clear();
    // endpoints.add(redisURI);
    // return connectPubSub(newStringStringCodec());
    // }

    public <K, V> StatefulRedisMultiDbPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec) {

        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }

        Map<RedisURI, RedisDatabase<StatefulRedisPubSubConnection<K, V>>> databases = new ConcurrentHashMap<>(endpoints.size());
        for (RedisURI uri : endpoints) {
            StatefulRedisPubSubConnection<K, V> connection = connectPubSub(codec, uri);
            databases.put(uri, new RedisDatabase<>(uri, 1 / getChannelCount(), connection, getCommandQueue(connection)));
        }

        return new StatefulRedisMultiDbPubSubConnectionImpl<K, V>(databases, getResources(), codec,
                getOptions().getJsonParser());
    }

    private ManagedCommandQueue getCommandQueue(StatefulRedisConnection<?, ?> connection) {
        RedisChannelWriter writer = ((StatefulRedisConnectionImpl<?, ?>) connection).getChannelWriter();
        if (writer instanceof Delegating) {
            writer = (RedisChannelWriter) ((Delegating<?>) writer).unwrap();
        }
        return (ManagedCommandQueue) writer;
    }

    // @Override
    // public void addListener(RedisConnectionStateListener listener) {
    // delegate.addListener(listener);
    // }

    // @Override
    // public void removeListener(RedisConnectionStateListener listener) {
    // delegate.removeListener(listener);
    // }

    // @Override
    // public void addListener(CommandListener listener) {
    // delegate.addListener(listener);
    // }

    // @Override
    // public void removeListener(CommandListener listener) {
    // delegate.removeListener(listener);
    // }

    // public void shutdown() {
    // delegate.shutdown(0, 2, TimeUnit.SECONDS);
    // super.shutdown(0, 2, TimeUnit.SECONDS);
    // }

    // @Override
    // public void close() {
    // delegate.close();
    // super.close();
    // }

    // public void shutdown(Duration quietPeriod, Duration timeout) {
    // delegate.shutdown(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
    // super.shutdown(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
    // }

    // @Override
    // public void shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {
    // delegate.shutdown(quietPeriod, timeout, timeUnit);
    // super.shutdown(quietPeriod, timeout, timeUnit);
    // }

    // public CompletableFuture<Void> shutdownAsync() {
    // return delegate.shutdownAsync().thenCompose(v -> super.shutdownAsync());
    // }

    // public CompletableFuture<Void> shutdownAsync(long quietPeriod, long timeout, TimeUnit timeUnit) {
    // return delegate.shutdownAsync(quietPeriod, timeout, timeUnit)
    // .thenCompose(v -> super.shutdownAsync(quietPeriod, timeout, timeUnit));
    // }

    @Override
    protected DefaultEndpoint createEndpoint() {
        return new AdvancedEndpoint(getOptions(), getResources());
    }

    @Override
    protected <K, V> PubSubEndpoint<K, V> createPubSubEndpoint() {
        return new AdvancedPubSubEndpoint<>(getOptions(), getResources());
    }

}
