package io.lettuce.core.failover;

import java.util.List;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisException;
import io.lettuce.core.protocol.DefaultEndpoint;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;

public class AdvancedEndpoint extends DefaultEndpoint {

    public AdvancedEndpoint(ClientOptions clientOptions, ClientResources clientResources) {
        super(clientOptions, clientResources);
    }

    void handOverCommandQueue(AdvancedEndpoint target) {
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
