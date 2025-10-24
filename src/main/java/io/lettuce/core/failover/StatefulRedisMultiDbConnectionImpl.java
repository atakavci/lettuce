package io.lettuce.core.failover;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.FutureSyncInvocationHandler;
import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.RedisConnectionStateListener;
import io.lettuce.core.RedisDatabase;
import io.lettuce.core.RedisReactiveCommandsImpl;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.push.PushListener;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import io.lettuce.core.internal.AbstractInvocationHandler;
import io.lettuce.core.json.JsonParser;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;

/**
 * Stateful connection wrapper that holds multiple underlying connections and delegates to the currently active one. Command
 * interfaces (sync/async/reactive) are dynamic proxies that always target the current active connection at invocation time so
 * they remain valid across switches.
 */
public class StatefulRedisMultiDbConnectionImpl<C extends StatefulRedisConnection<K, V>, K, V>
        implements StatefulRedisMultiDbConnection<K, V> {

    protected final Map<RedisURI, RedisDatabase<C>> databases;

    protected RedisDatabase<C> current;

    protected final RedisCommands<K, V> sync;

    protected final RedisAsyncCommandsImpl<K, V> async;

    protected final RedisReactiveCommandsImpl<K, V> reactive;

    protected final RedisCodec<K, V> codec;

    private final Supplier<JsonParser> parser;

    private final Set<PushListener> pushListeners = ConcurrentHashMap.newKeySet();

    private final Set<RedisConnectionStateListener> connectionStateListeners = ConcurrentHashMap.newKeySet();

    // private final ClientResources clientResources;

    public StatefulRedisMultiDbConnectionImpl(Map<RedisURI, RedisDatabase<C>> connections, ClientResources resources,
            RedisCodec<K, V> codec, Supplier<JsonParser> parser) {
        if (connections == null || connections.isEmpty()) {
            throw new IllegalArgumentException("connections must not be empty");
        }
        this.databases = new ConcurrentHashMap<>(connections);
        this.codec = codec;
        this.parser = parser;
        this.current = connections.values().stream().max(Comparator.comparingDouble(RedisDatabase::getWeight)).get();

        this.async = newRedisAsyncCommandsImpl();
        this.sync = newRedisSyncCommandsImpl();
        this.reactive = newRedisReactiveCommandsImpl();

    }

    @Override
    public RedisAsyncCommands<K, V> async() {
        return async;
    }

    /**
     * Create a new instance of {@link RedisCommands}. Can be overriden to extend.
     *
     * @return a new instance
     */
    protected RedisCommands<K, V> newRedisSyncCommandsImpl() {
        return syncHandler(async(), RedisCommands.class, RedisClusterCommands.class);
    }

    @SuppressWarnings("unchecked")
    protected <T> T syncHandler(Object asyncApi, Class<?>... interfaces) {
        AbstractInvocationHandler h = new FutureSyncInvocationHandler(this, asyncApi, interfaces);
        return (T) Proxy.newProxyInstance(AbstractRedisClient.class.getClassLoader(), interfaces, h);
    }

    /**
     * Create a new instance of {@link RedisAsyncCommandsImpl}. Can be overriden to extend.
     *
     * @return a new instance
     */
    protected RedisAsyncCommandsImpl<K, V> newRedisAsyncCommandsImpl() {
        return new RedisAsyncCommandsImpl<>(this, codec, parser);
    }

    @Override
    public RedisReactiveCommands<K, V> reactive() {
        return reactive;
    }

    /**
     * Create a new instance of {@link RedisReactiveCommandsImpl}. Can be overriden to extend.
     *
     * @return a new instance
     */
    protected RedisReactiveCommandsImpl<K, V> newRedisReactiveCommandsImpl() {
        return new RedisReactiveCommandsImpl<>(this, codec, parser);
    }

    @Override
    public RedisCommands<K, V> sync() {
        return sync;
    }

    @Override
    public void addListener(RedisConnectionStateListener listener) {
        connectionStateListeners.add(listener);
        current.getConnection().addListener(listener);
    }

    @Override
    public void removeListener(RedisConnectionStateListener listener) {
        connectionStateListeners.remove(listener);
        current.getConnection().removeListener(listener);
    }

    @Override
    public void setTimeout(Duration timeout) {
        databases.values().forEach(db -> db.getConnection().setTimeout(timeout));
    }

    @Override
    public Duration getTimeout() {
        return current.getConnection().getTimeout();
    }

    @Override
    public <T> RedisCommand<K, V, T> dispatch(RedisCommand<K, V, T> command) {
        return current.getConnection().dispatch(command);
    }

    @Override
    public Collection<RedisCommand<K, V, ?>> dispatch(Collection<? extends RedisCommand<K, V, ?>> commands) {
        return current.getConnection().dispatch(commands);
    }

    @Override
    public void close() {
        databases.values().forEach(db -> db.getConnection().close());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.allOf(databases.values().stream().map(db -> db.getConnection())
                .map(StatefulConnection::closeAsync).toArray(CompletableFuture[]::new));
    }

    @Override
    public boolean isOpen() {
        return current.getConnection().isOpen();
    }

    @Override
    public ClientOptions getOptions() {
        return current.getConnection().getOptions();
    }

    @Override
    public ClientResources getResources() {
        return current.getConnection().getResources();
    }

    @Override
    public void setAutoFlushCommands(boolean autoFlush) {
        databases.values().forEach(db -> db.getConnection().setAutoFlushCommands(autoFlush));
    }

    @Override
    public void flushCommands() {
        current.getConnection().flushCommands();
    }

    @Override
    public boolean isMulti() {
        return current.getConnection().isMulti();
    }

    @Override
    public void addListener(PushListener listener) {
        pushListeners.add(listener);
        current.getConnection().addListener(listener);
    }

    @Override
    public void removeListener(PushListener listener) {
        pushListeners.remove(listener);
        current.getConnection().removeListener(listener);
    }

    @Override
    public RedisURI getCurrentEndpoint() {
        return current.getRedisURI();
    }

    @Override
    public Iterable<RedisURI> getEndpoints() {
        return databases.keySet();
    }

    @Override
    public void switchToDatabase(RedisURI redisURI) {
        RedisDatabase<C> fromDb = current;
        RedisDatabase<C> toDb = databases.get(redisURI);
        if (fromDb == null || toDb == null) {
            throw new UnsupportedOperationException("Cannot initiate switch without a current and target database!");
        }
        current = toDb;
        connectionStateListeners.forEach(listener -> {
            toDb.getConnection().addListener(listener);
            fromDb.getConnection().removeListener(listener);
        });
        pushListeners.forEach(listener -> {
            toDb.getConnection().addListener(listener);
            fromDb.getConnection().removeListener(listener);
        });
        fromDb.getCommandQueue().handOverCommandQueue(toDb.getCommandQueue());
    }

}
