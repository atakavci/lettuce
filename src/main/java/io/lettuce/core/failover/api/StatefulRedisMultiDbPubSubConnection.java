package io.lettuce.core.failover.api;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

/**
 * Stateful multi-database Redis PubSub connection that supports failover between multiple endpoints. Each endpoint has its own
 * circuit breaker for tracking command metrics.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface StatefulRedisMultiDbPubSubConnection<K, V> extends StatefulRedisPubSubConnection<K, V>, BaseRedisMultiDb {

}
