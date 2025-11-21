package io.lettuce.core.failover;

import static io.lettuce.TestTags.INTEGRATION_TEST;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import io.lettuce.core.AbstractRedisClientTest;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import io.lettuce.test.WithPassword;
import io.lettuce.test.settings.TestSettings;

/**
 * Integration tests for MultiDb client failover using Toxiproxy for network failure simulation.
 * <p>
 * These tests were migrated from the original MultiDbClient design and adapted to the current API.
 * <p>
 * <b>API Differences from Original Tests:</b>
 * <ul>
 * <li>Original: {@code multiDbClient.setActive(uri)} → Current: {@code connection.switchToDatabase(uri)}</li>
 * <li>Original: {@code multiDbClient.getActive()} → Current: {@code connection.getCurrentEndpoint()}</li>
 * <li>Original: {@code multiDbClient.getEndpoints()} returns {@code RedisEndpoints} → Current: returns
 * {@code Iterable<RedisURI>}</li>
 * <li>Original: {@code multiDbClient.removeEndpoint(uri)} → Current: NOT SUPPORTED</li>
 * </ul>
 *
 * @author Mark Paluch (original)
 * @author Ivo Gaydazhiev (original)
 */
@Tag(INTEGRATION_TEST)
class MultiDbClientFailoverIntegrationTests extends AbstractRedisClientTest {

    // Backing redis instances
    private static final int redis1_port = TestSettings.port(8);

    private static final int redis2_port = TestSettings.port(9);

    // Redis Endpoints exposed by toxiproxy
    private static final RedisURI redis1ProxyUri = RedisURI.Builder.redis(host, TestSettings.proxyPort()).withPassword(passwd)
            .build();

    private static final RedisURI redis2ProxyUri = RedisURI.Builder.redis(host, TestSettings.proxyPort(1)).withPassword(passwd)
            .build();

    // Redis Endpoints directly connecting to the backing redis instances
    private static final RedisURI redis1Uri = RedisURI.Builder.redis(host, redis1_port).build();

    private static final RedisURI redis2Uri = RedisURI.Builder.redis(host, redis2_port).build();

    public static final String TESTKEY = "testkey";

    // Map of proxy endpoints to backing redis instances
    private static Map<RedisURI, RedisURI> proxyEndpointMap = new HashMap<>();
    static {
        proxyEndpointMap.put(redis1ProxyUri, redis1Uri);
        proxyEndpointMap.put(redis2ProxyUri, redis2Uri);
    }

    private RedisCommands<String, String> redis1Conn;

    private RedisCommands<String, String> redis2Conn;

    private static final Pattern pattern = Pattern.compile("tcp_port:(\\d+)");

    private static final ToxiproxyClient tp = new ToxiproxyClient("localhost", TestSettings.proxyAdminPort());

    private static Proxy redisProxy1;

    private static Proxy redisProxy2;

    // Map of proxy endpoints to backing redis instances
    private Map<RedisURI, RedisCommands<String, String>> commands = new HashMap<>();

    @BeforeAll
    public static void setupToxiproxy() throws IOException {
        if (tp.getProxyOrNull("redis-1") != null) {
            tp.getProxy("redis-1").delete();
        }
        if (tp.getProxyOrNull("redis-2") != null) {
            tp.getProxy("redis-2").delete();
        }

        redisProxy1 = tp.createProxy("redis-1", "0.0.0.0:" + TestSettings.proxyPort(), "redis-failover:" + redis1_port);
        redisProxy2 = tp.createProxy("redis-2", "0.0.0.0:" + TestSettings.proxyPort(1), "redis-failover:" + redis2_port);
    }

    @AfterAll
    public static void cleanupToxiproxy() throws IOException {
        if (redisProxy1 != null)
            redisProxy1.delete();
        if (redisProxy2 != null)
            redisProxy2.delete();
    }

    @BeforeEach
    public void enableAllToxiproxy() throws IOException {
        tp.getProxies().forEach(proxy -> {
            try {
                proxy.enable();
                for (Toxic toxic : proxy.toxics().getAll()) {
                    toxic.remove();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @BeforeEach
    void before() {
        redis1Conn = client.connect(redis1Uri).sync();
        redis2Conn = client.connect(redis2Uri).sync();

        commands.put(redis1Uri, redis1Conn);
        commands.put(redis2Uri, redis2Conn);

        WithPassword.enableAuthentication(this.redis1Conn);
        this.redis1Conn.auth(passwd);

        WithPassword.enableAuthentication(this.redis2Conn);
        this.redis2Conn.auth(passwd);

        redis1Conn.del(TESTKEY);
        redis2Conn.del(TESTKEY);
    }

    @AfterEach
    void after() {

        if (redis1Conn != null) {
            WithPassword.disableAuthentication(redis1Conn);
            redis1Conn.configRewrite();
            redis1Conn.getStatefulConnection().close();
        }

        if (redis2Conn != null) {
            WithPassword.disableAuthentication(redis2Conn);
            redis2Conn.configRewrite();
            redis2Conn.getStatefulConnection().close();
        }

    }

    @Test
    void testMultiDbSwitchActive() {
        MultiDbClient multiDbClient = MultiDbClient
                .create(MultiDbTestSupport.getDatabaseConfigs(redis1ProxyUri, redis2ProxyUri));
        ClientOptions clientOptions = ClientOptions.builder()
                .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(2)).build()).build();
        // Cast to MultiDbClientImpl to access setOptions (inherited from RedisClient)
        ((MultiDbClientImpl) multiDbClient).setOptions(clientOptions);

        try (StatefulRedisMultiDbConnection<String, String> connection = multiDbClient.connect(StringCodec.UTF8)) {

            String server = connection.sync().info("server");
            assertServerIs(server, redis1ProxyUri);

            // API CHANGE: Original used multiDbClient.setActive(redis2ProxyUri)
            connection.switchToDatabase(redis2ProxyUri);

            server = connection.sync().info("server");
            assertServerIs(server, redis2ProxyUri);
        } finally {
            multiDbClient.shutdown();
        }
    }

    @Test
    void testMultipleConnectionsSwitchActiveEndpoint() {
        MultiDbClient multiDbClient = MultiDbClient
                .create(MultiDbTestSupport.getDatabaseConfigs(redis1ProxyUri, redis2ProxyUri));
        ClientOptions clientOptions = ClientOptions.builder()
                .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofSeconds(2)).build()).build();
        // Cast to MultiDbClientImpl to access setOptions (inherited from RedisClient)
        ((MultiDbClientImpl) multiDbClient).setOptions(clientOptions);

        try (StatefulRedisMultiDbConnection<String, String> connection1 = multiDbClient.connect(StringCodec.UTF8);
                StatefulRedisMultiDbConnection<String, String> connection2 = multiDbClient.connect(StringCodec.UTF8);
                StatefulRedisMultiDbConnection<String, String> connection3 = multiDbClient.connect(StringCodec.UTF8)) {

            // Initially all connections should route to the first endpoint (east or west)
            String server1 = connection1.sync().info("server");
            String server2 = connection2.sync().info("server");
            String server3 = connection3.sync().info("server");

            // All should be on the same initial endpoint
            // API CHANGE: Original used multiDbClient.getActive()
            RedisURI initialActive = connection1.getCurrentEndpoint();
            assertServerIs(server1, initialActive);
            assertServerIs(server2, initialActive);
            assertServerIs(server3, initialActive);

            // Switch to the other endpoint
            RedisURI newActive = initialActive.equals(redis1ProxyUri) ? redis2ProxyUri : redis1ProxyUri;
            // API CHANGE: Original used multiDbClient.setActive(newActive)
            connection1.switchToDatabase(newActive);
            connection2.switchToDatabase(newActive);
            connection3.switchToDatabase(newActive);

            // All connections should now route to the new active endpoint
            server1 = connection1.sync().info("server");
            server2 = connection2.sync().info("server");
            server3 = connection3.sync().info("server");

            assertServerIs(server1, newActive);
            assertServerIs(server2, newActive);
            assertServerIs(server3, newActive);

            // Switch back to the original endpoint
            connection1.switchToDatabase(initialActive);
            connection2.switchToDatabase(initialActive);
            connection3.switchToDatabase(initialActive);

            // All connections should route back to the original endpoint
            server1 = connection1.sync().info("server");
            server2 = connection2.sync().info("server");
            server3 = connection3.sync().info("server");

            assertServerIs(server1, initialActive);
            assertServerIs(server2, initialActive);
            assertServerIs(server3, initialActive);

        } finally {
            multiDbClient.shutdown();
        }
    }

    @Test
    @Disabled("@atakavci: this is not a valid test case since endpoints can not be removed when active, and commands should not be requeued for inactive endpoints!")
    void testPendingCommandsHandledAfterEndpointRemoval() throws Exception {
        // Given: A MultiDb client with dynamic database support
        MultiDbClient dynamicClient = MultiDbClient
                .create(MultiDbTestSupport.getDatabaseConfigs(redis1ProxyUri, redis2ProxyUri));

        try {
            StatefulRedisMultiDbConnection<String, String> conn = dynamicClient.connect(StringCodec.UTF8);

            // Add a third database dynamically
            RedisURI redis3Uri = RedisURI.Builder.redis(host, TestSettings.port(10)).withPassword(passwd).build();
            conn.addDatabase(redis3Uri, 1.0f);

            // Verify the database was added
            List<RedisURI> endpoints = new ArrayList<>();
            conn.getEndpoints().forEach(endpoints::add);
            assertThat(endpoints).hasSize(3);
            assertThat(endpoints).contains(redis3Uri);

            // Switch to the third database and write a test value
            conn.switchToDatabase(redis3Uri);
            conn.async().set("test-key", "test-value").get(1, TimeUnit.SECONDS);

            // Switch back to first database
            conn.switchToDatabase(redis1ProxyUri);

            // When: Remove the third database
            conn.removeDatabase(redis3Uri);

            // Then: The database should be removed
            endpoints.clear();
            conn.getEndpoints().forEach(endpoints::add);
            assertThat(endpoints).hasSize(2);
            assertThat(endpoints).doesNotContain(redis3Uri);

            // Verify we can still use the remaining databases
            String value = conn.async().get("test-key").get(1, TimeUnit.SECONDS);
            assertThat(value).isNull(); // Different database, so key doesn't exist

            conn.close();
        } finally {
            dynamicClient.shutdown();
        }
    }

    private void assertServerIs(String server, RedisURI endpoint) {
        Matcher matcher = pattern.matcher(server);

        RedisURI expected = proxyEndpointMap.get(endpoint);
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("" + expected.getPort());
    }

}
