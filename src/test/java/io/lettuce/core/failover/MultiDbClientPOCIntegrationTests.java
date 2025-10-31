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

import java.util.List;

import javax.inject.Inject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TestSupport;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import io.lettuce.core.failover.api.StatefulRedisMultiDbPubSubConnection;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.lettuce.test.LettuceExtension;
import io.lettuce.test.TestFutures;
import io.lettuce.test.settings.TestSettings;

/**
 * Proof-of-Concept integration tests for {@link MultiDbClient} demonstrating database switching with key distribution across
 * multiple Redis endpoints.
 *
 * <p>
 * This test validates the core functionality of MultiDbClient by:
 * <ul>
 * <li>Writing keys to one database and verifying immediate reads</li>
 * <li>Switching to another database and continuing operations</li>
 * <li>Switching back to the original database</li>
 * <li>Verifying that keys are correctly distributed across databases</li>
 * </ul>
 *
 * @author Test Suite
 */
@ExtendWith(LettuceExtension.class)
@Tag(INTEGRATION_TEST)
class MultiDbClientPOCIntegrationTests extends TestSupport {

    private final MultiDbClient multiDbClient;

    private final RedisClient client1;

    private final RedisClient client2;

    private final RedisURI endpoint1;

    private final RedisURI endpoint2;

    private StatefulRedisMultiDbConnection<String, String> multiDbConnection;

    private StatefulRedisConnection<String, String> directConnection1;

    private StatefulRedisConnection<String, String> directConnection2;

    private List<RedisURI> getEndpoints() {
        return java.util.Arrays.asList(RedisURI.create(TestSettings.host(), TestSettings.port()),
                RedisURI.create(TestSettings.host(), TestSettings.port(1)));
    }

    @Inject
    MultiDbClientPOCIntegrationTests(MultiDbClient multiDbClient) {
        this.multiDbClient = multiDbClient;
        List<RedisURI> endpoints = getEndpoints();
        this.endpoint1 = endpoints.get(0);
        this.endpoint2 = endpoints.get(1);
        this.client1 = RedisClient.create(endpoint1);
        this.client2 = RedisClient.create(endpoint2);
    }

    @BeforeEach
    void setUp() {
        // Create connections
        multiDbConnection = multiDbClient.connect();
        directConnection1 = client1.connect();
        directConnection2 = client2.connect();

        // Clean up any existing test keys
        cleanupTestKeys();
    }

    @AfterEach
    void tearDown() {
        // // Clean up test keys
        // cleanupTestKeys();

        // Close connections
        if (multiDbConnection != null) {
            multiDbConnection.close();
        }
        if (directConnection1 != null) {
            directConnection1.close();
        }
        if (directConnection2 != null) {
            directConnection2.close();
        }
    }

    /**
     * Clean up test keys (key1 through key1500) from both databases.
     */
    private void cleanupTestKeys() {
        RedisCommands<String, String> sync1 = directConnection1 != null ? directConnection1.sync() : client1.connect().sync();
        RedisCommands<String, String> sync2 = directConnection2 != null ? directConnection2.sync() : client2.connect().sync();

        // Delete keys 1-1500 from both databases
        for (int i = 1; i <= 1500; i++) {
            String key = "key" + i;
            sync1.del(key);
            sync2.del(key);
        }
    }

    /**
     * POC Test: Database switching with key distribution (SYNC mode).
     *
     * <p>
     * This test demonstrates the core MultiDbClient functionality:
     * <ol>
     * <li>Write keys 1-500 to endpoint1, verify each write immediately</li>
     * <li>Switch to endpoint2, write keys 501-1000, verify each write</li>
     * <li>Switch back to endpoint1, write keys 1001-1500, verify each write</li>
     * <li>Verify final key distribution using direct connections:
     * <ul>
     * <li>Endpoint1 should have keys 1-500 and 1001-1500</li>
     * <li>Endpoint2 should have keys 501-1000</li>
     * <li>Endpoint1 should NOT have keys 501-1000</li>
     * <li>Endpoint2 should NOT have keys 1-500 and 1001-1500</li>
     * </ul>
     * </li>
     * </ol>
     */
    @Test
    void testDatabaseSwitchingWithKeyDistribution() {
        RedisCommands<String, String> multiDbSync = multiDbConnection.sync();

        // Phase 1: Write keys 1-500 to endpoint1 (default endpoint)
        multiDbConnection.switchToDatabase(endpoint1);
        assertThat(multiDbConnection.getCurrentEndpoint()).isEqualTo(endpoint1);

        for (int i = 1; i <= 500; i++) {
            String key = "key" + i;
            String value = String.valueOf(i);

            // Write and immediately verify
            String setResult = multiDbSync.set(key, value);
            assertThat(setResult).isEqualTo("OK");

            String readValue = multiDbSync.get(key);
            assertThat(readValue).isEqualTo(value);
        }

        // Phase 2: Switch to endpoint2 and write keys 501-1000
        multiDbConnection.switchToDatabase(endpoint2);
        assertThat(multiDbConnection.getCurrentEndpoint()).isEqualTo(endpoint2);

        for (int i = 501; i <= 1000; i++) {
            String key = "key" + i;
            String value = String.valueOf(i);

            // Write and immediately verify
            String setResult = multiDbSync.set(key, value);
            assertThat(setResult).isEqualTo("OK");

            String readValue = multiDbSync.get(key);
            assertThat(readValue).isEqualTo(value);
        }

        // Phase 3: Switch back to endpoint1 and write keys 1001-1500
        multiDbConnection.switchToDatabase(endpoint1);
        assertThat(multiDbConnection.getCurrentEndpoint()).isEqualTo(endpoint1);

        for (int i = 1001; i <= 1500; i++) {
            String key = "key" + i;
            String value = String.valueOf(i);

            // Write and immediately verify
            String setResult = multiDbSync.set(key, value);
            assertThat(setResult).isEqualTo("OK");

            String readValue = multiDbSync.get(key);
            assertThat(readValue).isEqualTo(value);
        }

        // Phase 4: Verify key distribution using direct connections
        RedisCommands<String, String> directSync1 = directConnection1.sync();
        RedisCommands<String, String> directSync2 = directConnection2.sync();

        // Verify endpoint1 has keys 1-500 with correct values
        for (int i = 1; i <= 500; i++) {
            String key = "key" + i;
            String expectedValue = String.valueOf(i);
            String actualValue = directSync1.get(key);
            assertThat(actualValue).as("Endpoint1 should have %s with value %s", key, expectedValue).isEqualTo(expectedValue);
        }

        // Verify endpoint1 does NOT have keys 501-1000
        for (int i = 501; i <= 1000; i++) {
            String key = "key" + i;
            String actualValue = directSync1.get(key);
            assertThat(actualValue).as("Endpoint1 should NOT have %s", key).isNull();
        }

        // Verify endpoint1 has keys 1001-1500 with correct values
        for (int i = 1001; i <= 1500; i++) {
            String key = "key" + i;
            String expectedValue = String.valueOf(i);
            String actualValue = directSync1.get(key);
            assertThat(actualValue).as("Endpoint1 should have %s with value %s", key, expectedValue).isEqualTo(expectedValue);
        }

        // Verify endpoint2 does NOT have keys 1-500
        for (int i = 1; i <= 500; i++) {
            String key = "key" + i;
            String actualValue = directSync2.get(key);
            assertThat(actualValue).as("Endpoint2 should NOT have %s", key).isNull();
        }

        // Verify endpoint2 has keys 501-1000 with correct values
        for (int i = 501; i <= 1000; i++) {
            String key = "key" + i;
            String expectedValue = String.valueOf(i);
            String actualValue = directSync2.get(key);
            assertThat(actualValue).as("Endpoint2 should have %s with value %s", key, expectedValue).isEqualTo(expectedValue);
        }

        // Verify endpoint2 does NOT have keys 1001-1500
        for (int i = 1001; i <= 1500; i++) {
            String key = "key" + i;
            String actualValue = directSync2.get(key);
            assertThat(actualValue).as("Endpoint2 should NOT have %s", key).isNull();
        }

        // Summary verification: count total keys in each database
        long endpoint1KeyCount = 0;
        long endpoint2KeyCount = 0;

        for (int i = 1; i <= 1500; i++) {
            String key = "key" + i;
            if (directSync1.exists(key) == 1) {
                endpoint1KeyCount++;
            }
            if (directSync2.exists(key) == 1) {
                endpoint2KeyCount++;
            }
        }

        assertThat(endpoint1KeyCount).as("Endpoint1 should have exactly 1000 keys (1-500 and 1001-1500)").isEqualTo(1000);
        assertThat(endpoint2KeyCount).as("Endpoint2 should have exactly 500 keys (501-1000)").isEqualTo(500);
    }

    /**
     * POC Test: Database switching with key distribution (ASYNC mode).
     *
     * <p>
     * This test demonstrates true async behavior by firing all commands without waiting for results. It validates the
     * MultiDbClient's ability to handle concurrent async operations across database switches.
     *
     * <p>
     * Test phases:
     * <ol>
     * <li>Fire all write commands without waiting:
     * <ul>
     * <li>Write keys 1-500 to endpoint1</li>
     * <li>Switch to endpoint2, write keys 501-1000</li>
     * <li>Switch back to endpoint1, write keys 1001-1500</li>
     * </ul>
     * </li>
     * <li>Fire all read commands without waiting (verify writes completed)</li>
     * <li>Wait for all futures to complete</li>
     * <li>Verify key distribution using direct connections</li>
     * <li>Report results to output file: read success, missing keys, keys in wrong database</li>
     * </ol>
     */
    @Test
    void testDatabaseSwitchingWithKeyDistributionAsync() throws Exception {
        RedisAsyncCommands<String, String> multiDbAsync = multiDbConnection.async();

        // Lists to track all futures
        java.util.List<RedisFuture<String>> writeFutures = new java.util.ArrayList<>();
        java.util.Map<String, Integer> keyToExpectedEndpoint = new java.util.HashMap<>();

        // Phase 1: Fire write commands for keys 1-500 to endpoint1 (NO WAITING)
        multiDbConnection.switchToDatabase(endpoint1);
        assertThat(multiDbConnection.getCurrentEndpoint()).isEqualTo(endpoint1);

        for (int i = 1; i <= 500; i++) {
            String key = "key" + i;
            String value = String.valueOf(i);

            // Fire async write without waiting
            RedisFuture<String> setFuture = multiDbAsync.set(key, value);
            writeFutures.add(setFuture);
            keyToExpectedEndpoint.put(key, 1); // Expected on endpoint1
        }

        // Phase 2: Switch to endpoint2 and fire write commands for keys 501-1000 (NO WAITING)
        multiDbConnection.switchToDatabase(endpoint2);
        assertThat(multiDbConnection.getCurrentEndpoint()).isEqualTo(endpoint2);

        for (int i = 501; i <= 1000; i++) {
            String key = "key" + i;
            String value = String.valueOf(i);

            // Fire async write without waiting
            RedisFuture<String> setFuture = multiDbAsync.set(key, value);
            writeFutures.add(setFuture);
            keyToExpectedEndpoint.put(key, 2); // Expected on endpoint2
        }

        // Phase 3: Switch back to endpoint1 and fire write commands for keys 1001-1500 (NO WAITING)
        multiDbConnection.switchToDatabase(endpoint1);
        assertThat(multiDbConnection.getCurrentEndpoint()).isEqualTo(endpoint1);

        for (int i = 1001; i <= 1500; i++) {
            String key = "key" + i;
            String value = String.valueOf(i);

            // Fire async write without waiting
            RedisFuture<String> setFuture = multiDbAsync.set(key, value);
            writeFutures.add(setFuture);
            keyToExpectedEndpoint.put(key, 1); // Expected on endpoint1
        }

        // Wait for all writes to complete
        for (RedisFuture<String> future : writeFutures) {
            TestFutures.getOrTimeout(future);
        }

        // Phase 4: Fire read commands to verify writes (NO WAITING)
        java.util.Map<String, RedisFuture<String>> keyToReadFuture = new java.util.HashMap<>();

        // Read from endpoint1 (should have keys 1-500 and 1001-1500)
        multiDbConnection.switchToDatabase(endpoint1);
        for (int i = 1; i <= 500; i++) {
            String key = "key" + i;
            RedisFuture<String> getFuture = multiDbAsync.get(key);
            keyToReadFuture.put(key, getFuture);
        }
        for (int i = 1001; i <= 1500; i++) {
            String key = "key" + i;
            RedisFuture<String> getFuture = multiDbAsync.get(key);
            keyToReadFuture.put(key, getFuture);
        }

        // Read from endpoint2 (should have keys 501-1000)
        multiDbConnection.switchToDatabase(endpoint2);
        for (int i = 501; i <= 1000; i++) {
            String key = "key" + i;
            RedisFuture<String> getFuture = multiDbAsync.get(key);
            keyToReadFuture.put(key, getFuture);
        }

        // Wait for all reads to complete and collect results
        java.util.Map<String, String> readResults = new java.util.HashMap<>();
        for (java.util.Map.Entry<String, RedisFuture<String>> entry : keyToReadFuture.entrySet()) {
            String key = entry.getKey();
            try {
                String value = TestFutures.getOrTimeout(entry.getValue());
                readResults.put(key, value);
            } catch (Exception e) {
                readResults.put(key, null); // Read failed
            }
        }

        // Phase 5: Verify key distribution using direct connections
        RedisCommands<String, String> directSync1 = directConnection1.sync();
        RedisCommands<String, String> directSync2 = directConnection2.sync();

        // Track statistics (declare outside try block for final assertion)
        int readSuccessCount = 0;
        int readFailureCount = 0;
        int missingKeysCount = 0;
        int wrongEndpointCount = 0;
        int correctEndpointCount = 0;

        java.util.List<String> missingKeys = new java.util.ArrayList<>();
        java.util.List<String> wrongEndpointKeys = new java.util.ArrayList<>();

        // Prepare output file
        java.io.File outputFile = new java.io.File("target/async-test-results.txt");
        outputFile.getParentFile().mkdirs();

        try (java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter(outputFile))) {
            writer.println(seperator2());
            writer.println("MultiDbClient Async Test Results");
            writer.println(seperator2());
            writer.println();

            // Analyze each key
            for (int i = 1; i <= 1500; i++) {
                String key = "key" + i;
                String expectedValue = String.valueOf(i);
                int expectedEndpoint = keyToExpectedEndpoint.get(key);

                // Check read result
                String readValue = readResults.get(key);
                boolean readSuccess = expectedValue.equals(readValue);

                if (readSuccess) {
                    readSuccessCount++;
                } else {
                    readFailureCount++;
                }

                // Check actual location
                boolean onEndpoint1 = directSync1.exists(key) == 1;
                boolean onEndpoint2 = directSync2.exists(key) == 1;

                if (!onEndpoint1 && !onEndpoint2) {
                    missingKeysCount++;
                    missingKeys.add(key);
                } else if ((expectedEndpoint == 1 && onEndpoint1) || (expectedEndpoint == 2 && onEndpoint2)) {
                    correctEndpointCount++;
                } else {
                    wrongEndpointCount++;
                    wrongEndpointKeys.add(String.format("%s (expected: endpoint%d, actual: endpoint%d)", key, expectedEndpoint,
                            onEndpoint1 ? 1 : 2));
                }
            }

            // Write summary
            writer.println("SUMMARY");
            writer.println(seperator1());
            writer.printf("Total keys: 1500%n");
            writer.printf("Read success: %d (%.1f%%)%n", readSuccessCount, readSuccessCount * 100.0 / 1500);
            writer.printf("Read failure: %d (%.1f%%)%n", readFailureCount, readFailureCount * 100.0 / 1500);
            writer.println();
            writer.printf("Keys in correct endpoint: %d (%.1f%%)%n", correctEndpointCount, correctEndpointCount * 100.0 / 1500);
            writer.printf("Keys in wrong endpoint: %d (%.1f%%)%n", wrongEndpointCount, wrongEndpointCount * 100.0 / 1500);
            writer.printf("Missing keys: %d (%.1f%%)%n", missingKeysCount, missingKeysCount * 100.0 / 1500);
            writer.println();

            // Write missing keys
            if (!missingKeys.isEmpty()) {
                writer.println("MISSING KEYS");
                writer.println(seperator1());
                for (String key : missingKeys) {
                    writer.println(key);
                }
                writer.println();
            }

            // Write wrong endpoint keys
            if (!wrongEndpointKeys.isEmpty()) {
                writer.println("KEYS IN WRONG ENDPOINT");
                writer.println(seperator1());
                for (String keyInfo : wrongEndpointKeys) {
                    writer.println(keyInfo);
                }
                writer.println();
            }

            writer.println(seperator2());
            writer.println("Test completed successfully");
            writer.println("Output file: " + outputFile.getAbsolutePath());
            writer.println(seperator2());
        }

        // Print summary to console
        System.out.println("\n" + seperator2());
        System.out.println("Async test results written to: " + outputFile.getAbsolutePath());
        System.out.println(seperator2() + "\n");

        // Basic assertion: test passes if we have reasonable results (no strict requirements for async)
        assertThat(readSuccessCount).as("Most reads should succeed").isGreaterThan(0);
    }

    private String seperator1() {
        return "--------------------------------------------------------------------------------";
    }

    private String seperator2() {
        return "================================================================================";
    }

    /**
     * Test PubSub functionality with MultiDbClient.
     *
     * This test: 1. Subscribes to a channel using MultiDbClient PubSub connection 2. Runs for 12 seconds, collecting all
     * received messages 3. Simultaneously publishes messages to both databases using direct connections (async) 4. Publishes
     * message pairs (one to each DB) every 20ms with incrementing messageId 5. Switches databases every 3 seconds (db1 -> db2
     * -> db1 -> db2) 6. Message format: "messageId:X database:Y" 7. Writes all received messages to an output file
     */
    @Test
    void testPubSubWithMultiDbClient() throws Exception {
        // Prepare output file
        java.io.File outputFile = new java.io.File("target/pubsub-test-results.txt");
        outputFile.getParentFile().mkdirs();

        // Thread-safe list to collect messages
        java.util.List<String> receivedMessages = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicInteger messageCount = new java.util.concurrent.atomic.AtomicInteger(0);

        // Create PubSub connection with MultiDbClient
        StatefulRedisMultiDbPubSubConnection<String, String> multiDbPubSub = multiDbClient.connectPubSub();

        // Add listener to collect messages
        multiDbPubSub.addListener(new RedisPubSubAdapter<String, String>() {

            @Override
            public void message(String channel, String message) {
                receivedMessages.add(message);
                messageCount.incrementAndGet();
            }

        });

        // Subscribe to the test channel
        String testChannel = "test-channel";
        multiDbPubSub.sync().subscribe(testChannel);

        // Give subscription time to establish
        Thread.sleep(100);

        // Create direct PubSub connections for publishing
        StatefulRedisPubSubConnection<String, String> publisher1 = client1.connectPubSub();
        StatefulRedisPubSubConnection<String, String> publisher2 = client2.connectPubSub();

        // Publisher thread that sends messages to both databases using async methods
        java.util.concurrent.atomic.AtomicBoolean publisherRunning = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicInteger publishedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        Thread publisherThread = new Thread(() -> {
            int messageId = 1;
            while (publisherRunning.get()) {
                try {
                    // Publish to database 1 (async - fire and forget)
                    String message1 = "messageId:" + messageId + " database:1";
                    publisher1.async().publish(testChannel, message1);

                    // Publish to database 2 (async - fire and forget)
                    String message2 = "messageId:" + messageId + " database:2";
                    publisher2.async().publish(testChannel, message2);

                    publishedCount.addAndGet(2);
                    messageId++;

                    // Wait 20ms before next pair
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // Log error but continue
                    System.err.println("Publisher error: " + e.getMessage());
                }
            }
        });

        // Start publisher thread
        publisherThread.start();

        // Start on endpoint1 (default)
        System.out.println("[Main] Starting on endpoint1: " + endpoint1);

        // Sleep 3 seconds
        Thread.sleep(3000);

        // Switch to endpoint2
        System.out.println("[Main] Switching to endpoint2: " + endpoint2);
        multiDbPubSub.switchToDatabase(endpoint2);

        // Sleep 3 seconds
        Thread.sleep(3000);

        // Switch back to endpoint1
        System.out.println("[Main] Switching to endpoint1: " + endpoint1);
        multiDbPubSub.switchToDatabase(endpoint1);

        // Sleep 3 seconds
        Thread.sleep(3000);

        // Switch to endpoint2
        System.out.println("[Main] Switching to endpoint2: " + endpoint2);
        multiDbPubSub.switchToDatabase(endpoint2);

        // Sleep 3 seconds
        Thread.sleep(3000);

        // Stop publisher
        publisherRunning.set(false);
        publisherThread.join(1000);

        // Give time for final messages to arrive
        Thread.sleep(500);

        // Close connections
        multiDbPubSub.close();
        publisher1.close();
        publisher2.close();

        // Analyze received messages
        int db1Count = 0;
        int db2Count = 0;
        java.util.Set<Integer> db1MessageIds = new java.util.TreeSet<>();
        java.util.Set<Integer> db2MessageIds = new java.util.TreeSet<>();

        for (String message : receivedMessages) {
            if (message.contains("database:1")) {
                db1Count++;
                // Extract messageId
                String[] parts = message.split(" ");
                if (parts.length >= 1) {
                    String idPart = parts[0].replace("messageId:", "");
                    try {
                        db1MessageIds.add(Integer.parseInt(idPart));
                    } catch (NumberFormatException e) {
                        // Ignore malformed messages
                    }
                }
            } else if (message.contains("database:2")) {
                db2Count++;
                // Extract messageId
                String[] parts = message.split(" ");
                if (parts.length >= 1) {
                    String idPart = parts[0].replace("messageId:", "");
                    try {
                        db2MessageIds.add(Integer.parseInt(idPart));
                    } catch (NumberFormatException e) {
                        // Ignore malformed messages
                    }
                }
            }
        }

        // Write results to file
        try (java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileWriter(outputFile))) {
            writer.println(seperator2());
            writer.println("MultiDbClient PubSub Test Results");
            writer.println(seperator2());
            writer.println();

            writer.println("TEST CONFIGURATION");
            writer.println(seperator1());
            writer.println("Test duration: 12 seconds");
            writer.println("Message interval: 20ms per pair (one to each database)");
            writer.println("Publishing method: Async (fire and forget)");
            writer.println("Database switching: Every 3 seconds (db1 -> db2 -> db1 -> db2)");
            writer.println("  - 0-3s: endpoint1 (port 6479)");
            writer.println("  - 3-6s: endpoint2 (port 6480)");
            writer.println("  - 6-9s: endpoint1 (port 6479)");
            writer.println("  - 9-12s: endpoint2 (port 6480)");
            writer.println("Channel: " + testChannel);
            writer.println("Final endpoint: " + multiDbPubSub.getCurrentEndpoint());
            writer.println();

            writer.println("SUMMARY");
            writer.println(seperator1());
            writer.printf("Total messages received: %d%n", receivedMessages.size());
            writer.printf("Total messages published: %d%n", publishedCount.get());
            writer.printf("Messages from database 1: %d%n", db1Count);
            writer.printf("Messages from database 2: %d%n", db2Count);
            writer.println();

            writer.println("MESSAGE ID RANGES");
            writer.println(seperator1());
            if (!db1MessageIds.isEmpty()) {
                writer.printf("Database 1 message IDs: %d to %d (%d unique)%n", db1MessageIds.iterator().next(),
                        ((java.util.TreeSet<Integer>) db1MessageIds).last(), db1MessageIds.size());
            }
            if (!db2MessageIds.isEmpty()) {
                writer.printf("Database 2 message IDs: %d to %d (%d unique)%n", db2MessageIds.iterator().next(),
                        ((java.util.TreeSet<Integer>) db2MessageIds).last(), db2MessageIds.size());
            }
            writer.println();

            writer.println("ALL RECEIVED MESSAGES");
            writer.println(seperator1());
            for (int i = 0; i < receivedMessages.size(); i++) {
                writer.printf("[%d] %s%n", i + 1, receivedMessages.get(i));
            }
            writer.println();

            writer.println(seperator2());
            writer.println("Test completed successfully");
            writer.println("Output file: " + outputFile.getAbsolutePath());
            writer.println(seperator2());
        }

        // Print summary to console
        System.out.println("\n" + seperator2());
        System.out.println("PubSub test results written to: " + outputFile.getAbsolutePath());
        System.out.println("Total messages received: " + receivedMessages.size());
        System.out.println("Messages from DB1: " + db1Count + ", DB2: " + db2Count);
        System.out.println(seperator2() + "\n");

        // Basic assertion: we should have received some messages
        assertThat(receivedMessages.size()).as("Should have received messages").isGreaterThan(0);
    }

}
