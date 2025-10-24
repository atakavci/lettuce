package io.lettuce.core.failover;

import io.lettuce.core.protocol.RedisCommand;

public interface ManagedCommandQueue {

    void handOverCommandQueue(ManagedCommandQueue target);

    <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command);

}
