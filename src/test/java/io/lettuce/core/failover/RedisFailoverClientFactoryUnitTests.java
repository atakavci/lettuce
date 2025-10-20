package io.lettuce.core.failover;

import static io.lettuce.TestTags.UNIT_TEST;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisURI;
import io.lettuce.test.resource.FastShutdown;
import io.lettuce.test.resource.TestClientResources;
import io.lettuce.test.settings.TestSettings;

/**
 * @author Mark Paluch
 */
@Tag(UNIT_TEST)
class RedisFailoverClientFactoryUnitTests {

    private List<RedisURI> getEndpoints() {
        return java.util.Arrays.asList(RedisURI.create(TestSettings.host(), TestSettings.port()),
                RedisURI.create(TestSettings.host(), TestSettings.port(1)));
    }

    private static final RedisURI REDIS_URI = RedisURI.create(TestSettings.host(), TestSettings.port());

    @Test
    void plain() {
        FastShutdown.shutdown(RedisFailoverClient.create(getEndpoints()));
    }

    @Test
    void withStringUri() {
        FastShutdown.shutdown(RedisFailoverClient.create(getEndpoints()));
    }

    // @Test
    // void withStringUriNull() {
    // assertThatThrownBy(() -> RedisFailoverClient.create((String) null)).isInstanceOf(IllegalArgumentException.class);
    // }

    @Test
    void withUri() {
        FastShutdown.shutdown(RedisFailoverClient.create(getEndpoints()));
    }

    @Test
    void withUriNull() {
        assertThatThrownBy(() -> RedisFailoverClient.create(Collections.singletonList((RedisURI) null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // @Test
    // void clientResources() {
    //     FastShutdown.shutdown(RedisFailoverClient.create(TestClientResources.get()));
    // }

    // @Test
    // void clientResourcesNull() {
    // assertThatThrownBy(() -> RedisFailoverClient.create((ClientResources) null))
    // .isInstanceOf(IllegalArgumentException.class);
    // }

    // @Test
    // void clientResourcesWithStringUri() {
    // FastShutdown.shutdown(RedisFailoverClient.create(TestClientResources.get(), URI));
    // }

    // @Test
    // void clientResourcesWithStringUriNull() {
    // assertThatThrownBy(() -> RedisFailoverClient.create(TestClientResources.get(), (String) null))
    // .isInstanceOf(IllegalArgumentException.class);
    // }

    // @Test
    // void clientResourcesNullWithStringUri() {
    // assertThatThrownBy(() -> RedisFailoverClient.create(null, URI)).isInstanceOf(IllegalArgumentException.class);
    // }

    @Test
    void clientResourcesWithUri() {
        FastShutdown.shutdown(RedisFailoverClient.create(TestClientResources.get(), Collections.singletonList(REDIS_URI)));
    }

    @Test
    void clientResourcesWithUriNull() {
        assertThatThrownBy(
                () -> RedisFailoverClient.create(TestClientResources.get(), Collections.singletonList((RedisURI) null)))
                        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void clientResourcesNullWithUri() {
        assertThatThrownBy(() -> RedisFailoverClient.create(null, Collections.singletonList(REDIS_URI)))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
