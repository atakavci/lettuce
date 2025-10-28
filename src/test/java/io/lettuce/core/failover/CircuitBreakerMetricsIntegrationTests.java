package io.lettuce.core.failover;

import static io.lettuce.test.settings.TestSettings.host;
import static io.lettuce.test.settings.TestSettings.port;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TestSupport;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import io.lettuce.test.LettuceExtension;

/**
 * Integration tests for circuit breaker metrics tracking in multi-database connections.
 *
 * @author Augment
 */
@ExtendWith(LettuceExtension.class)
@Tag("integration")
class CircuitBreakerMetricsIntegrationTests extends TestSupport {

    private final MultiDbClient client;

    private final RedisClient client1;

    private final RedisClient client2;

    CircuitBreakerMetricsIntegrationTests() {
        List<RedisURI> endpoints = getEndpoints();
        this.client = MultiDbClient.create(endpoints);
        this.client1 = RedisClient.create(endpoints.get(0));
        this.client2 = RedisClient.create(endpoints.get(1));
    }

    private List<RedisURI> getEndpoints() {
        return Arrays.asList(RedisURI.create(host(), port()), RedisURI.create(host(), port(1)));
    }

    @BeforeEach
    void setUp() {
        client1.connect().sync().flushall();
        client2.connect().sync().flushall();
    }

    @AfterEach
    void tearDown() {
        client1.shutdown();
        client2.shutdown();
    }

    @Test
    void shouldTrackSuccessfulCommands() {
        StatefulRedisMultiDbConnection<String, String> connection = client.connect();
        RedisURI endpoint = connection.getCurrentEndpoint();

        // Execute successful command
        connection.sync().set("key", "value");

        // Get metrics
        CircuitBreaker cb = connection.getCircuitBreaker(endpoint);
        assertNotNull(cb);
        assertThat(cb.getSuccessCount()).isGreaterThanOrEqualTo(1);
        assertThat(cb.getFailureCount()).isEqualTo(0);

        connection.close();
    }

    @Test
    void shouldTrackMultipleCommands() {
        StatefulRedisMultiDbConnection<String, String> connection = client.connect();
        RedisURI endpoint = connection.getCurrentEndpoint();

        // Execute multiple commands
        connection.sync().set("key1", "value1");
        connection.sync().set("key2", "value2");
        connection.sync().get("key1");

        // Get metrics
        CircuitBreaker cb = connection.getCircuitBreaker(endpoint);
        assertThat(cb.getSuccessCount()).isGreaterThanOrEqualTo(3);
        assertThat(cb.getFailureCount()).isEqualTo(0);

        connection.close();
    }

    @Test
    void shouldIsolatMetricsPerEndpoint() {
        StatefulRedisMultiDbConnection<String, String> connection = client.connect();
        List<RedisURI> endpoints = StreamSupport.stream(connection.getEndpoints().spliterator(), false)
                .collect(java.util.stream.Collectors.toList());

        // Execute command on first endpoint
        connection.sync().set("key1", "value1");
        RedisURI firstEndpoint = connection.getCurrentEndpoint();

        // Switch to second endpoint
        RedisURI secondEndpoint = endpoints.stream().filter(uri -> !uri.equals(firstEndpoint)).findFirst()
                .orElseThrow(() -> new IllegalStateException("No second endpoint found"));
        connection.switchToDatabase(secondEndpoint);

        // Execute command on second endpoint
        connection.sync().set("key2", "value2");

        // Get metrics for both endpoints
        CircuitBreaker cb1 = connection.getCircuitBreaker(firstEndpoint);
        CircuitBreaker cb2 = connection.getCircuitBreaker(secondEndpoint);

        // Verify isolation - each endpoint has its own metrics
        assertThat(cb1.getSuccessCount()).isGreaterThanOrEqualTo(1);
        assertThat(cb2.getSuccessCount()).isGreaterThanOrEqualTo(1);

        connection.close();
    }

    @Test
    void shouldThrowExceptionForUnknownEndpoint() {
        StatefulRedisMultiDbConnection<String, String> connection = client.connect();

        RedisURI unknownEndpoint = RedisURI.create("redis://unknown:9999");

        assertThatThrownBy(() -> connection.getCircuitBreaker(unknownEndpoint)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown endpoint");

        connection.close();
    }

    @Test
    void shouldMaintainMetricsAfterSwitch() {
        StatefulRedisMultiDbConnection<String, String> connection = client.connect();
        RedisURI firstEndpoint = connection.getCurrentEndpoint();

        // Execute command on first endpoint
        connection.sync().set("key1", "value1");
        CircuitBreaker cb1Before = connection.getCircuitBreaker(firstEndpoint);
        long successes1Before = cb1Before.getSuccessCount();

        // Switch to second endpoint
        List<RedisURI> endpoints = StreamSupport.stream(connection.getEndpoints().spliterator(), false)
                .collect(java.util.stream.Collectors.toList());
        RedisURI secondEndpoint = endpoints.stream().filter(uri -> !uri.equals(firstEndpoint)).findFirst()
                .orElseThrow(() -> new IllegalStateException("No second endpoint found"));
        connection.switchToDatabase(secondEndpoint);

        // Execute command on second endpoint
        connection.sync().set("key2", "value2");

        // Switch back to first endpoint
        connection.switchToDatabase(firstEndpoint);

        // Verify metrics for first endpoint are unchanged
        CircuitBreaker cb1After = connection.getCircuitBreaker(firstEndpoint);
        assertThat(cb1After.getSuccessCount()).isEqualTo(successes1Before);

        connection.close();
    }

    @Test
    void shouldExposeMetricsViaCircuitBreaker() {
        StatefulRedisMultiDbConnection<String, String> connection = client.connect();
        RedisURI endpoint = connection.getCurrentEndpoint();

        // Execute commands
        connection.sync().set("key", "value");
        connection.sync().get("key");

        // Get circuit breaker and verify metrics are accessible
        CircuitBreaker cb = connection.getCircuitBreaker(endpoint);
        assertNotNull(cb);
        assertNotNull(cb.getMetrics());
        assertThat(cb.getSuccessCount()).isGreaterThanOrEqualTo(2);

        connection.close();
    }

}
