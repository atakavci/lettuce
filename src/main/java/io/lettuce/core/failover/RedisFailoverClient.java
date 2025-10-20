package io.lettuce.core.failover;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisDatabase;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.event.command.CommandListener;
import io.lettuce.core.failover.api.StatefulRedisFailoverConnection;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.resource.ClientResources;

/**
 * Failover-aware client that composes multiple standalone Redis endpoints and returns a single Stateful connection wrapper
 * which can switch the active endpoint without requiring users to recreate command objects.
 *
 * Standalone-only POC. Not for Sentinel/Cluster.
 */
public class RedisFailoverClient extends AbstractRedisClient {

    private final List<RedisURI> endpoints;

    private final RedisClient delegate;

    protected RedisFailoverClient(ClientResources clientResources, List<RedisURI> endpoints) {
        super(clientResources);

        delegate = RedisClient.create(this.getResources());
        this.endpoints = (endpoints == null) ? new ArrayList<>() : new ArrayList<>(endpoints);
        this.endpoints.forEach(uri -> LettuceAssert.notNull(uri, "RedisURI must not be null"));
    }

    public static RedisFailoverClient create() {
        return new RedisFailoverClient(null, null);
    }

    public static RedisFailoverClient create(List<RedisURI> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }
        return new RedisFailoverClient(null, endpoints);
    }

    public static RedisFailoverClient create(ClientResources resources, List<RedisURI> endpoints) {
        if (resources == null) {
            throw new IllegalArgumentException("resources must not be null");
        }
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }
        return new RedisFailoverClient(resources, endpoints);
    }

    /**
     * Open a new connection to a Redis server that treats keys and values as UTF-8 strings.
     *
     * @return A new stateful Redis connection
     */
    public StatefulRedisFailoverConnection<String, String> connect() {
        return connect(newStringStringCodec());
    }

    public StatefulRedisConnection<String, String> connect(RedisURI redisURI) {
        endpoints.clear();
        endpoints.add(redisURI);
        return connect(newStringStringCodec());
    }

    public <K, V> StatefulRedisFailoverConnection<K, V> connect(RedisCodec<K, V> codec, RedisURI redisURI) {
        endpoints.clear();
        endpoints.add(redisURI);
        return connect(codec);
    }

    public <K, V> StatefulRedisFailoverConnection<K, V> connect(RedisCodec<K, V> codec) {

        if (codec == null) {
            throw new IllegalArgumentException("codec must not be null");
        }

        Map<RedisURI, RedisDatabase<K, V>> connections = new ConcurrentHashMap<>(endpoints.size());
        float weight = 1.0f;
        for (RedisURI uri : endpoints) {
            // HACK: looks like repeating the implementation all around 'RedisClient.connect' is an overkill.
            // connections.put(uri, connect(codec, uri));
            // Instead we will use it from delegate
            connections.put(uri, new RedisDatabase(uri, weight, delegate.connect(codec, uri)));
            weight = +2;
        }

        return new StatefulRedisFailoverConnectionImpl<>(connections, getResources(), codec, getOptions().getJsonParser());
    }

    protected RedisCodec<String, String> newStringStringCodec() {
        return StringCodec.UTF8;
    }

    @Override
    public void addListener(RedisConnectionStateListener listener) {
        delegate.addListener(listener);
    }

    @Override
    public void removeListener(RedisConnectionStateListener listener) {
        delegate.removeListener(listener);
    }

    @Override
    public void addListener(CommandListener listener) {
        delegate.addListener(listener);
    }

    @Override
    public void removeListener(CommandListener listener) {
        delegate.removeListener(listener);
    }

    public void shutdown() {
        delegate.shutdown(0, 2, TimeUnit.SECONDS);
        super.shutdown(0, 2, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        delegate.close();
        super.close();
    }

    public void shutdown(Duration quietPeriod, Duration timeout) {
        delegate.shutdown(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
        super.shutdown(quietPeriod.toNanos(), timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void shutdown(long quietPeriod, long timeout, TimeUnit timeUnit) {
        delegate.shutdown(quietPeriod, timeout, timeUnit);
        super.shutdown(quietPeriod, timeout, timeUnit);
    }

    public CompletableFuture<Void> shutdownAsync() {
        return delegate.shutdownAsync().thenCompose(v -> super.shutdownAsync());
    }

    public CompletableFuture<Void> shutdownAsync(long quietPeriod, long timeout, TimeUnit timeUnit) {
        return delegate.shutdownAsync(quietPeriod, timeout, timeUnit)
                .thenCompose(v -> super.shutdownAsync(quietPeriod, timeout, timeUnit));
    }

}
