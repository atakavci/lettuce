package io.lettuce.core.failover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;

/**
 * Unit tests for MultiDbClient.
 * <p>
 * These tests were migrated from the original MultiDbClient design and adapted to the current API.
 * <p>
 * <b>API Differences from Original Tests:</b>
 * <ul>
 * <li>Original: {@code multiDbClient.setActive(uri)} → Current: {@code connection.switchToDatabase(uri)}</li>
 * <li>Original: {@code multiDbClient.getActive()} → Current: {@code connection.getCurrentEndpoint()}</li>
 * <li>Original: {@code multiDbClient.getEndpoints()} returns {@code RedisEndpoints} → Current: returns
 * {@code Iterable<RedisURI>}</li>
 * <li>Original: {@code multiDbClient.addEndpoint(uri)} → Current: NOT SUPPORTED</li>
 * <li>Original: {@code multiDbClient.removeEndpoint(uri)} → Current: NOT SUPPORTED</li>
 * </ul>
 * <p>
 * <b>Tests marked with {@code @Disabled("API_NOT_COMPATIBLE: ...")} cannot be migrated because the current API does not support
 * dynamic endpoint management (add/remove).</b>
 *
 * @author Mark Paluch (original)
 * @author Ivo Gaydazhiev (original)
 */
class MultiDbClientUnitTests {

    private MultiDbClient client;

    private StatefulRedisMultiDbConnection<String, String> connection;

    @AfterEach
    void tearDown() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    void shouldCreateWithMultipleEndpoints() {
        RedisURI uri1 = RedisURI.create("redis://localhost:6379");
        RedisURI uri2 = RedisURI.create("redis://localhost:6380");

        client = MultiDbClient.create(MultiDbTestSupport.getDatabaseConfigs(uri1, uri2));

        assertThat(client).isNotNull();
    }

    @Test
    void shouldCreateWithSet() {
        client = MultiDbClient.create(MultiDbTestSupport.getDatabaseConfigs());

        assertThat(client).isNotNull();
    }

    @Test
    void shouldRejectNullDatabaseConfigs() {
        assertThatThrownBy(() -> MultiDbClient.create((List<DatabaseConfig>) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectEmptyRedisURIs() {
        assertThatThrownBy(() -> MultiDbClient.create(Arrays.asList())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Disabled("REQUIRES_REDIS: This test requires actual Redis instances running - should be moved to integration tests")
    void shouldSetActiveEndpoint() {
        RedisURI uri1 = RedisURI.create("redis://localhost:6379");
        RedisURI uri2 = RedisURI.create("redis://localhost:6380");

        client = MultiDbClient.create(MultiDbTestSupport.getDatabaseConfigs(uri1, uri2));
        connection = client.connect();

        // API CHANGE: Original used multiDbClient.setActive(uri2)
        connection.switchToDatabase(uri2);

        // API CHANGE: Original used multiDbClient.getActive()
        assertThat(connection.getCurrentEndpoint()).isEqualTo(uri2);
    }

    @Test
    @Disabled("REQUIRES_REDIS: This test requires actual Redis instances running - should be moved to integration tests")
    void shouldRejectSettingNonExistentEndpointAsActive() {
        RedisURI uri1 = RedisURI.create("redis://localhost:6379");
        RedisURI uri2 = RedisURI.create("redis://localhost:6380");
        RedisURI uri3 = RedisURI.create("redis://localhost:6381");

        client = MultiDbClient.create(MultiDbTestSupport.getDatabaseConfigs(uri1, uri2, uri3));
        connection = client.connect();

        // API CHANGE: Original used multiDbClient.setActive(uri3)
        assertThatThrownBy(() -> connection.switchToDatabase(uri3)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @Disabled("REQUIRES_REDIS: This test requires actual Redis instances running - should be moved to integration tests")
    void shouldGetActiveEndpoint() {
        RedisURI uri1 = RedisURI.create("redis://localhost:6379");
        RedisURI uri2 = RedisURI.create("redis://localhost:6380");

        client = MultiDbClient.create(MultiDbTestSupport.getDatabaseConfigs(uri1, uri2));
        connection = client.connect();

        // API CHANGE: Original used multiDbClient.getActive()
        RedisURI active = connection.getCurrentEndpoint();
        assertThat(active).isIn(uri1, uri2);
    }

    @Test
    @Disabled("API_NOT_COMPATIBLE: addEndpoint() not supported in current design - endpoints are fixed at client creation time")
    void shouldAddEndpoint() {
        // This test cannot be migrated because the current API does not support dynamic endpoint addition
        // Original test used: multiDbClient.addEndpoint(uri)
        // Current API: Endpoints are fixed when the client is created
    }

    @Test
    @Disabled("API_NOT_COMPATIBLE: addEndpoint() not supported in current design - endpoints are fixed at client creation time")
    void shouldNotAddDuplicateEndpoint() {
        // This test cannot be migrated because the current API does not support dynamic endpoint addition
        // Original test used: multiDbClient.addEndpoint(uri)
        // Current API: Endpoints are fixed when the client is created
    }

    @Test
    @Disabled("API_NOT_COMPATIBLE: removeEndpoint() not supported in current design - endpoints are fixed at client creation time")
    void shouldRemoveEndpoint() {
        // This test cannot be migrated because the current API does not support dynamic endpoint removal
        // Original test used: multiDbClient.removeEndpoint(uri)
        // Current API: Endpoints are fixed when the client is created
    }

    @Test
    @Disabled("API_NOT_COMPATIBLE: removeEndpoint() not supported in current design - endpoints are fixed at client creation time")
    void shouldNotRemoveNonExistentEndpoint() {
        // This test cannot be migrated because the current API does not support dynamic endpoint removal
        // Original test used: multiDbClient.removeEndpoint(uri)
        // Current API: Endpoints are fixed when the client is created
    }

    @Test
    @Disabled("API_NOT_COMPATIBLE: removeEndpoint() not supported in current design - endpoints are fixed at client creation time")
    void shouldNotRemoveLastEndpoint() {
        // This test cannot be migrated because the current API does not support dynamic endpoint removal
        // Original test used: multiDbClient.removeEndpoint(uri)
        // Current API: Endpoints are fixed when the client is created
    }

    @Test
    @Disabled("REQUIRES_REDIS: This test requires actual Redis instances running - should be moved to integration tests")
    void shouldSwitchActiveEndpointDynamically() {
        RedisURI uri1 = RedisURI.create("redis://localhost:6379");
        RedisURI uri2 = RedisURI.create("redis://localhost:6380");

        client = MultiDbClient.create(MultiDbTestSupport.getDatabaseConfigs(uri1, uri2));
        connection = client.connect();

        // API CHANGE: Original used multiDbClient.setActive(uri1)
        connection.switchToDatabase(uri1);
        // API CHANGE: Original used multiDbClient.getActive()
        assertThat(connection.getCurrentEndpoint()).isEqualTo(uri1);

        // API CHANGE: Original used multiDbClient.setActive(uri2)
        connection.switchToDatabase(uri2);
        // API CHANGE: Original used multiDbClient.getActive()
        assertThat(connection.getCurrentEndpoint()).isEqualTo(uri2);
    }

    @Test
    @Disabled("API_NOT_COMPATIBLE: addEndpoint() not supported in current design - endpoints are fixed at client creation time")
    void shouldAddEndpointAndSetItActive() {
        // This test cannot be migrated because the current API does not support dynamic endpoint addition
        // Original test used: multiDbClient.addEndpoint(uri) and multiDbClient.setActive(uri)
        // Current API: Endpoints are fixed when the client is created
    }

    @Test
    @Disabled("API_NOT_COMPATIBLE: removeEndpoint() not supported in current design - endpoints are fixed at client creation time")
    void shouldNotRemoveActiveEndpoint() {
        // This test cannot be migrated because the current API does not support dynamic endpoint removal
        // Original test used: multiDbClient.removeEndpoint(uri) where uri is the active endpoint
        // Current API: Endpoints are fixed when the client is created
    }

    @Test
    @Disabled("REQUIRES_REDIS: This test requires actual Redis instances running - should be moved to integration tests")
    void shouldGetEndpointsObject() {
        RedisURI uri1 = RedisURI.create("redis://localhost:6379");
        RedisURI uri2 = RedisURI.create("redis://localhost:6380");

        client = MultiDbClient.create(MultiDbTestSupport.getDatabaseConfigs(uri1, uri2));
        connection = client.connect();

        // API CHANGE: Original returned RedisEndpoints object with methods like getAll(), size(), etc.
        // Current API returns Iterable<RedisURI>
        Iterable<RedisURI> endpoints = connection.getEndpoints();
        assertThat(endpoints).isNotNull();

        // Convert to list to check contents
        List<RedisURI> endpointList = StreamSupport.stream(endpoints.spliterator(), false).collect(Collectors.toList());
        assertThat(endpointList).hasSize(2);
        assertThat(endpointList).containsExactlyInAnyOrder(uri1, uri2);
    }

}
