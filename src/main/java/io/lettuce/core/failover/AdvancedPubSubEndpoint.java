package io.lettuce.core.failover;

import java.util.List;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.pubsub.PubSubEndpoint;
import io.lettuce.core.resource.ClientResources;

public class AdvancedPubSubEndpoint<K, V> extends PubSubEndpoint<K, V> implements ManagedCommandQueue {

    public AdvancedPubSubEndpoint(ClientOptions clientOptions, ClientResources clientResources) {
        super(clientOptions, clientResources);
    }

    @Override
    public List<RedisCommand<?, ?, ?>> drainCommands() {
        return super.drainCommands();
    };

}
