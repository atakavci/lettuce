package io.lettuce.core.event;

import static io.lettuce.TestTags.UNIT_TEST;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Subscription;

import io.lettuce.test.Wait;
import reactor.core.scheduler.Schedulers;

/**
 * @author Mark Paluch
 */
@Tag(UNIT_TEST)
@ExtendWith(MockitoExtension.class)
class DefaultEventBusUnitTests {

    @Mock
    private Event event;

    @Test
    void publishToSubscriber() throws Exception {

        EventBus sut = new DefaultEventBus(Schedulers.immediate());

        Collection<Event> events = new LinkedBlockingQueue<>();
        Subscription subscription = sut.subscribe(Event.class, e -> events.add(e));

        sut.publish(event);

        // Wait for the event to be published
        Wait.untilEquals(1, events::size).waitOrTimeout();

        assertThat(events).hasSize(1);
        assertThat(events.iterator().next()).isEqualTo(event);

        subscription.cancel();
    }

    @Test
    void publishToMultipleSubscribers() throws Exception {

        EventBus sut = new DefaultEventBus(Schedulers.parallel());

        Collection<Event> events1 = new LinkedBlockingQueue<>();
        Collection<Event> events2 = new LinkedBlockingQueue<>();

        Subscription subscription1 = sut.subscribe(Event.class, e -> events1.add(e));
        Subscription subscription2 = sut.subscribe(Event.class, e -> events2.add(e));

        sut.publish(event);

        // Wait for both subscribers to receive the event
        Wait.untilEquals(1, events1::size).waitOrTimeout();
        Wait.untilEquals(1, events2::size).waitOrTimeout();

        assertThat(events1).hasSize(1);
        assertThat(events1.iterator().next()).isEqualTo(event);
        assertThat(events2).hasSize(1);
        assertThat(events2.iterator().next()).isEqualTo(event);

        subscription1.cancel();
        subscription2.cancel();
    }

}
