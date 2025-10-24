package io.lettuce.core.failover;

import java.util.List;

import io.lettuce.core.RedisException;
import io.lettuce.core.protocol.RedisCommand;

public interface ManagedCommandQueue {

    List<RedisCommand<?, ?, ?>> drainCommands();

    <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command);

    default void handOverCommandQueue(ManagedCommandQueue target) {
        List<RedisCommand<?, ?, ?>> commands = this.drainCommands();

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
