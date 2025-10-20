package io.lettuce.core.failover.api;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public interface StatefulRedisMultiDbPubSubConnection<K, V> extends StatefulRedisPubSubConnection<K, V>, BaseRedisMultiDb {

}
