package io.lettuce.core.failover;

import io.lettuce.core.FutureSyncInvocationHandler;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.internal.AbstractInvocationHandler;

class FailoverFutureSyncInvocationFactory extends FutureSyncInvocationHandler {

    FailoverFutureSyncInvocationFactory(StatefulConnection<?, ?> connection, Object asyncApi, Class<?>[] interfaces) {
        throw new UnsupportedOperationException();
    }

    static AbstractInvocationHandler createFutureSyncInvocationHandler(StatefulConnection<?, ?> connection, Object asyncApi,
            Class<?>[] interfaces) {
        return FutureSyncInvocationHandler.create(connection, asyncApi, interfaces);
    }

}
