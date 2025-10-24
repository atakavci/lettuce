package io.lettuce.core.failover;

import java.util.List;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.protocol.DefaultEndpoint;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;

public class AdvancedEndpoint extends DefaultEndpoint implements ManagedCommandQueue {

    public AdvancedEndpoint(ClientOptions clientOptions, ClientResources clientResources) {
        super(clientOptions, clientResources);
    }

    @Override
    public List<RedisCommand<?, ?, ?>> drainCommands() {
        return super.drainCommands();
    };

}
