package io.lettuce.core.event;

import static io.lettuce.TestTags.INTEGRATION_TEST;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import org.reactivestreams.Subscription;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.MyStreamingRedisCredentialsProvider;
import io.lettuce.core.event.connection.AuthenticationEvent;
import io.lettuce.core.event.connection.ReauthenticationEvent;
import io.lettuce.core.event.connection.ReauthenticationFailedEvent;
import io.lettuce.test.LettuceExtension;
import io.lettuce.test.WithPassword;
import io.lettuce.test.settings.TestSettings;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TestSupport;
import io.lettuce.core.event.connection.ConnectionEvent;
import io.lettuce.test.Wait;
import io.lettuce.test.resource.FastShutdown;
import io.lettuce.test.resource.TestClientResources;

/**
 * @author Mark Paluch
 * @author Ivo Gaydajiev
 */
@Tag(INTEGRATION_TEST)
@ExtendWith(LettuceExtension.class)
class ConnectionEventsTriggeredIntegrationTests extends TestSupport {

    @Test
    void testConnectionEvents() {

        RedisClient client = RedisClient.create(TestClientResources.get(), RedisURI.Builder.redis(host, port).build());

        Flux<ConnectionEvent> publisher = client.getResources().eventBus().get()
                .filter(event -> event instanceof ConnectionEvent).cast(ConnectionEvent.class);

        StepVerifier.create(publisher).then(() -> client.connect().close()).assertNext(event -> {
            assertThat(event.remoteAddress()).isNotNull();
            assertThat(event.localAddress()).isNotNull();
            assertThat(event.toString()).contains("->");
        }).expectNextCount(3).thenCancel().verify(Duration.of(5, ChronoUnit.SECONDS));

        FastShutdown.shutdown(client);
    }

    @Test
    void testReauthenticateEvents() {

        MyStreamingRedisCredentialsProvider credentialsProvider = new MyStreamingRedisCredentialsProvider();
        credentialsProvider.emitCredentials(TestSettings.username(), TestSettings.password().toString().toCharArray());

        RedisClient client = RedisClient.create(RedisURI.create(TestSettings.host(), TestSettings.port()));
        client.setOptions(ClientOptions.builder()
                .reauthenticateBehavior(ClientOptions.ReauthenticateBehavior.ON_NEW_CREDENTIALS).build());
        RedisURI uri = RedisURI.Builder.redis(host, port).withAuthentication(credentialsProvider).build();

        Flux<AuthenticationEvent> publisher = client.getResources().eventBus().get()
                .filter(event -> event instanceof AuthenticationEvent).cast(AuthenticationEvent.class);

        WithPassword.run(client, () -> StepVerifier.create(publisher).then(() -> client.connect(uri))
                .assertNext(event -> assertThat(event).asInstanceOf(InstanceOfAssertFactories.type(ReauthenticationEvent.class))
                        .extracting(ReauthenticationEvent::getEpId).isNotNull())
                .then(() -> credentialsProvider.emitCredentials(TestSettings.username(), "invalid".toCharArray()))
                .assertNext(event -> assertThat(event)
                        .asInstanceOf(InstanceOfAssertFactories.type(ReauthenticationFailedEvent.class))
                        .extracting(ReauthenticationFailedEvent::getEpId).isNotNull())
                .thenCancel().verify(Duration.of(1, ChronoUnit.SECONDS)));

        FastShutdown.shutdown(client);
    }

    @Test
    void testConnectionEventsWithPublisher() throws Exception {

        RedisClient client = RedisClient.create(TestClientResources.get(), RedisURI.Builder.redis(host, port).build());

        Collection<ConnectionEvent> events = new LinkedBlockingQueue<>();
        EventBus eventBus = client.getResources().eventBus();

        Subscription subscription = eventBus.subscribe(ConnectionEvent.class, event -> events.add(event));

        client.connect().close();

        // Wait for events to be published (expecting 4 events: Connected, Activated, Deactivated, Disconnected)
        Wait.untilEquals(4, events::size).waitOrTimeout();

        assertThat(events).hasSize(4);

        // Verify the first event has the expected properties
        ConnectionEvent firstEvent = events.iterator().next();
        assertThat(firstEvent.remoteAddress()).isNotNull();
        assertThat(firstEvent.localAddress()).isNotNull();
        assertThat(firstEvent.toString()).contains("->");

        subscription.cancel();
        FastShutdown.shutdown(client);
    }

    @Test
    void testReauthenticateEventsWithPublisher() throws Exception {

        MyStreamingRedisCredentialsProvider credentialsProvider = new MyStreamingRedisCredentialsProvider();
        credentialsProvider.emitCredentials(TestSettings.username(), TestSettings.password().toString().toCharArray());

        RedisClient client = RedisClient.create(RedisURI.create(TestSettings.host(), TestSettings.port()));
        client.setOptions(ClientOptions.builder()
                .reauthenticateBehavior(ClientOptions.ReauthenticateBehavior.ON_NEW_CREDENTIALS).build());
        RedisURI uri = RedisURI.Builder.redis(host, port).withAuthentication(credentialsProvider).build();

        Collection<AuthenticationEvent> events = new LinkedBlockingQueue<>();
        EventBus eventBus = client.getResources().eventBus();

        Subscription subscription = eventBus.subscribe(AuthenticationEvent.class, event -> events.add(event));

        WithPassword.run(client, () -> {
            client.connect(uri);

            // Wait for the first reauthentication event
            Wait.untilEquals(1, events::size).waitOrTimeout();

            assertThat(events).hasSize(1);
            AuthenticationEvent firstEvent = events.iterator().next();
            assertThat(firstEvent).asInstanceOf(InstanceOfAssertFactories.type(ReauthenticationEvent.class))
                    .extracting(ReauthenticationEvent::getEpId).isNotNull();

            // Emit invalid credentials to trigger a failed reauthentication
            credentialsProvider.emitCredentials(TestSettings.username(), "invalid".toCharArray());

            // Wait for the second event (failed reauthentication)
            Wait.untilEquals(2, events::size).waitOrTimeout();

            assertThat(events).hasSize(2);
            // Get the second event
            AuthenticationEvent secondEvent = events.stream().skip(1).findFirst()
                    .orElseThrow(() -> new AssertionError("Expected second event"));
            assertThat(secondEvent).asInstanceOf(InstanceOfAssertFactories.type(ReauthenticationFailedEvent.class))
                    .extracting(ReauthenticationFailedEvent::getEpId).isNotNull();
        });

        subscription.cancel();
        FastShutdown.shutdown(client);
    }

}
