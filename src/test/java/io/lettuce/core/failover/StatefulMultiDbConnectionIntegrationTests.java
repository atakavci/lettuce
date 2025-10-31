/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 *
 * This file contains contributions from third-party contributors
 * licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lettuce.core.failover;

import static io.lettuce.TestTags.INTEGRATION_TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.junit.After;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import io.lettuce.test.TestFutures;
import io.lettuce.test.LettuceExtension;

/**
 * Integration tests for {@link StatefulRedisMultiDbConnection} with basic commands and database switching.
 *
 * @author Test Suite
 */
@ExtendWith(LettuceExtension.class)
@Tag(INTEGRATION_TEST)
class StatefulMultiDbConnectionIntegrationTests extends MultiDbTestSupport {

    @Inject
    StatefulMultiDbConnectionIntegrationTests(MultiDbClient client) {
        super(client);
    }

    @BeforeEach
    void setUp() {
        directClient1.connect().sync().flushall();
        directClient2.connect().sync().flushall();
    }

    @After
    void tearDownAfter() {
        directClient1.shutdown();
        directClient2.shutdown();
    }

    // ============ Basic Connection Tests ============

    @Test
    void shouldConnectToMultipleEndpoints() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        assertNotNull(connection);
        assertThat(connection.getEndpoints()).isNotNull();
        connection.close();
    }

    @Test
    void shouldGetCurrentEndpoint() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        RedisURI currentEndpoint = connection.getCurrentEndpoint();
        assertNotNull(currentEndpoint);
        assertThat(currentEndpoint).isIn(connection.getEndpoints());
        connection.close();
    }

    @Test
    void shouldListAllEndpoints() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        Iterable<RedisURI> endpoints = connection.getEndpoints();
        assertThat(endpoints).isNotNull();
        assertThat(StreamSupport.stream(endpoints.spliterator(), false).count()).isGreaterThanOrEqualTo(2);
        connection.close();
    }

    // ============ Basic Command Tests (Sync) ============

    @Test
    void shouldSetAndGetValueSync() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        connection.sync().set("testKey", "testValue");
        String value = connection.sync().get("testKey");
        assertEquals("testValue", value);
        connection.close();
    }

    @Test
    void shouldDeleteKeySync() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        connection.sync().set("deleteKey", "value");
        Long deleted = connection.sync().del("deleteKey");
        assertEquals(1L, deleted);
        String value = connection.sync().get("deleteKey");
        assertNull(value);
        connection.close();
    }

    @Test
    void shouldIncrementValueSync() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        connection.sync().set("counter", "10");
        Long result = connection.sync().incr("counter");
        assertEquals(11L, result);
        connection.close();
    }

    @Test
    void shouldAppendToStringSync() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        connection.sync().set("mykey", "Hello");
        Long length = connection.sync().append("mykey", " World");
        assertEquals(11L, length);
        String value = connection.sync().get("mykey");
        assertEquals("Hello World", value);
        connection.close();
    }

    // ============ Basic Command Tests (Async) ============

    @Test
    void shouldSetAndGetValueAsync() throws Exception {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        RedisFuture<String> setFuture = connection.async().set("asyncKey", "asyncValue");
        TestFutures.awaitOrTimeout(setFuture);
        RedisFuture<String> getFuture = connection.async().get("asyncKey");
        TestFutures.awaitOrTimeout(getFuture);
        assertEquals("asyncValue", getFuture.get());
        connection.close();
    }

    @Test
    void shouldDeleteKeyAsync() throws Exception {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        connection.async().set("asyncDeleteKey", "value");
        RedisFuture<Long> deleteFuture = connection.async().del("asyncDeleteKey");
        TestFutures.awaitOrTimeout(deleteFuture);
        assertEquals(1L, deleteFuture.get());
        connection.close();
    }

    @Test
    void shouldIncrementValueAsync() throws Exception {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        connection.async().set("asyncCounter", "5");
        RedisFuture<Long> incrFuture = connection.async().incr("asyncCounter");
        TestFutures.awaitOrTimeout(incrFuture);
        assertEquals(6L, incrFuture.get());
        connection.close();
    }

    // ============ Database Switching Tests ============

    @Test
    void shouldSwitchBetweenDatabases() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();

        // Set value in first database
        connection.sync().set("switchKey", "value1");
        assertEquals("value1", connection.sync().get("switchKey"));

        // Switch to second database
        RedisURI other = StreamSupport.stream(connection.getEndpoints().spliterator(), false)
                .filter(uri -> !uri.equals(connection.getCurrentEndpoint())).findFirst().get();
        connection.switchToDatabase(other);

        // Value should not exist in second database
        assertNull(connection.sync().get("switchKey"));

        // Set different value in second database
        connection.sync().set("switchKey", "value2");
        assertEquals("value2", connection.sync().get("switchKey"));

        connection.close();
    }

    @Test
    void shouldMaintainDataAfterSwitch() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();

        // Set value in first database
        connection.sync().set("persistKey", "persistValue");
        RedisURI firstDb = connection.getCurrentEndpoint();

        // Switch to second database
        RedisURI secondDb = StreamSupport.stream(connection.getEndpoints().spliterator(), false)
                .filter(uri -> !uri.equals(firstDb)).findFirst().get();
        connection.switchToDatabase(secondDb);

        // Switch back to first database
        connection.switchToDatabase(firstDb);

        // Original value should still exist
        assertEquals("persistValue", connection.sync().get("persistKey"));

        connection.close();
    }

    @Test
    void shouldSwitchAndExecuteCommandsAsync() throws Exception {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();

        // Set value in first database
        RedisFuture<String> setFuture1 = connection.async().set("asyncSwitchKey", "asyncValue1");
        TestFutures.awaitOrTimeout(setFuture1);

        // Switch to second database
        RedisURI other = StreamSupport.stream(connection.getEndpoints().spliterator(), false)
                .filter(uri -> !uri.equals(connection.getCurrentEndpoint())).findFirst().get();
        connection.switchToDatabase(other);

        // Set different value in second database
        RedisFuture<String> setFuture2 = connection.async().set("asyncSwitchKey", "asyncValue2");
        TestFutures.awaitOrTimeout(setFuture2);

        // Get value from second database
        RedisFuture<String> getFuture = connection.async().get("asyncSwitchKey");
        TestFutures.awaitOrTimeout(getFuture);
        assertEquals("asyncValue2", getFuture.get());

        connection.close();
    }

    @Test
    void shouldHandleMultipleSwitches() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        RedisURI firstDb = connection.getCurrentEndpoint();
        RedisURI secondDb = StreamSupport.stream(connection.getEndpoints().spliterator(), false)
                .filter(uri -> !uri.equals(firstDb)).findFirst().get();

        // First database
        connection.sync().set("key", "value1");
        assertEquals("value1", connection.sync().get("key"));

        // Switch to second
        connection.switchToDatabase(secondDb);
        connection.sync().set("key", "value2");
        assertEquals("value2", connection.sync().get("key"));

        // Switch back to first
        connection.switchToDatabase(firstDb);
        assertEquals("value1", connection.sync().get("key"));

        // Switch to second again
        connection.switchToDatabase(secondDb);
        assertEquals("value2", connection.sync().get("key"));

        connection.close();
    }

    // ============ List Operations Tests ============

    @Test
    void shouldPushAndPopFromList() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        connection.sync().rpush("mylist", "a", "b", "c");
        String value = connection.sync().lpop("mylist");
        assertEquals("a", value);
        connection.close();
    }

    @Test
    void shouldGetListLength() {
        StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect();
        connection.sync().rpush("listlen", "one", "two", "three");
        Long length = connection.sync().llen("listlen");
        assertEquals(3L, length);
        connection.close();
    }

}
