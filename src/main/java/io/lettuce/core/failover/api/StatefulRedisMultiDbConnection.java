package io.lettuce.core.failover.api;

import io.lettuce.core.api.StatefulRedisConnection;

public interface StatefulRedisMultiDbConnection<K, V> extends StatefulRedisConnection<K, V>, BaseRedisMultiDb {


}
