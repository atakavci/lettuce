package io.lettuce.core.failover;

import java.util.Collection;

import io.lettuce.core.RedisException;
import io.lettuce.core.protocol.RedisCommand;

public interface ManagedCommandQueue {

    Collection<RedisCommand<?, ?, ?>> drainCommands();

    <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command);

    default void handOverCommandQueue(ManagedCommandQueue target) {
        Collection<RedisCommand<?, ?, ?>> commands = this.drainCommands();

        for (RedisCommand<?, ?, ?> queuedCommand : commands) {
            if (queuedCommand == null || queuedCommand.isCancelled()) {
                continue;
            }

            try {
                target.write(queuedCommand);
            } catch (RedisException e) {
                queuedCommand.completeExceptionally(e);
            }
        }
    }

}
