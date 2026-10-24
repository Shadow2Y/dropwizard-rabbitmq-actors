package io.appform.dropwizard.actors.retry;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.connectivity.PublishingChannel;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.observers.TerminalRMQObserver;
import io.appform.dropwizard.actors.retry.async.AsyncRetryScheduler;
import io.appform.dropwizard.actors.retry.async.RetrySchedulingRejectedException;
import io.appform.dropwizard.actors.retry.async.RetryState;
import io.appform.dropwizard.actors.retry.async.TtlRequeueRetryScheduler;
import io.appform.dropwizard.actors.retry.config.CountLimitedIncrementalWaitRetryConfig;
import io.appform.dropwizard.actors.retry.config.NoRetryConfig;
import io.appform.dropwizard.actors.retry.config.RetryConfig;
import io.dropwizard.util.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RetryStrategyFactoryTest {

    @Test
    void publisherHookWiresLifecycleAndConfirmedRetryDestination() throws Exception {
        var config = new NoRetryConfig();
        config.setAsyncConfirmTimeout(Duration.milliseconds(1234));
        var connection = mock(RMQConnection.class);
        when(connection.getRootObserver()).thenReturn(new TerminalRMQObserver());
        var publisher = new PublishingChannel(connection, 1234);
        var factory = new RetryStrategyFactory() {
            @Override
            protected PublishingChannel createRetryPublisher(RetryConfig actual, RMQConnection transport) {
                assertSame(config, actual);
                assertSame(connection, transport);
                return publisher;
            }
        };
        var scheduler = factory.createScheduler(config, new ActorConfig(), connection, "exchange", "queue");
        var topology = mock(Channel.class);
        var publishChannel = mock(Channel.class);
        when(publishChannel.isOpen()).thenReturn(true);
        when(connection.newChannel()).thenReturn(topology, publishChannel);
        scheduler.start();
        verify(connection, never()).newChannel();
        var body = new byte[]{1};
        scheduler.schedule("queue", body, new AMQP.BasicProperties(), new RetryState(2, 900), 100);
        verify(publishChannel).basicPublish(eq(TtlRequeueRetryScheduler.waitingQueueName("exchange", "classic", 100)),
                eq("queue"), eq(true), any(), same(body));
        verify(publishChannel).confirmSelect();
        verify(publishChannel).waitForConfirmsOrDie(1234);
        scheduler.stop();
        verify(publishChannel).close();
        verify(connection, times(2)).newChannel();
        verify(topology).close();
    }

    @Test
    void syncDoesNotConstructSchedulerOrRequireTransportContext() throws Exception {
        var calls = new AtomicInteger();
        var factory = new RetryStrategyFactory() {
            @Override
            protected AsyncRetryScheduler createScheduler(RetryConfig config, ActorConfig actorConfig,
                                                            RMQConnection connection, String exchange, String queueName) {
                calls.incrementAndGet();
                throw new AssertionError("Sync must not construct retry transport");
            }
        };

        var strategy = factory.create(new NoRetryConfig(), null, null, null, null);

        assertTrue(strategy.execute(() -> true, null, null, null));
        assertEquals(0, calls.get());
    }

    @Test
    void contextualAsyncCreationUsesOverriddenSchedulerHook() throws Exception {
        var scheduler = mock(AsyncRetryScheduler.class);
        var config = new NoRetryConfig();
        config.setWaitMode(WaitMode.ASYNC);
        var actorConfig = new ActorConfig();
        var connection = mock(RMQConnection.class);
        var factory = new RetryStrategyFactory() {
            @Override
            protected AsyncRetryScheduler createScheduler(RetryConfig actualConfig, ActorConfig actualActorConfig,
                                                            RMQConnection actualConnection, String exchange, String queueName) {
                assertSame(config, actualConfig);
                assertSame(actorConfig, actualActorConfig);
                assertSame(connection, actualConnection);
                assertEquals("exchange", exchange);
                assertEquals("queue", queueName);
                return scheduler;
            }
        };

        var strategy = factory.create(config, actorConfig, connection, "exchange", "queue");
        strategy.start();
        strategy.stop();

        verify(scheduler).start();
        verify(scheduler).stop();
        verifyNoInteractions(connection);
    }

    @Test
    void contextualSyncCreationPreservesSingleArgumentFactoryOverride() {
        var config = new NoRetryConfig();
        var customStrategy = mock(RetryStrategy.class);
        var factory = new RetryStrategyFactory() {
            @Override
            public RetryStrategy create(RetryConfig actual) {
                assertSame(config, actual);
                return customStrategy;
            }
        };

        assertSame(customStrategy, factory.create(config, null, null, null, null));
    }

    @Test
    void incrementalRetriesCanExceedThirtyTwoDelaysWithConfiguredLimit() throws Exception {
        var config = new CountLimitedIncrementalWaitRetryConfig();
        config.setWaitMode(WaitMode.ASYNC);
        config.setMaxAttempts(40);
        config.setAsyncMaxDelayLevels(39);
        var connection = mock(RMQConnection.class);
        var topology = mock(Channel.class);
        var publisher = mock(Channel.class);
        when(connection.getRootObserver()).thenReturn(new TerminalRMQObserver());
        when(publisher.isOpen()).thenReturn(true);
        when(connection.newChannel()).thenReturn(topology, publisher, topology);
        var strategy = new RetryStrategyFactory().create(config, new ActorConfig(), connection, "exchange", "queue");
        TtlRequeueRetryScheduler.setMaxRetryQueues(64);
        try {
            strategy.start();

            for (long attempt = 1; attempt < 40; attempt++) {
                var properties = new AMQP.BasicProperties.Builder().headers(Map.of(
                        "x-actor-retry-queue", "queue", "x-actor-retry-attempt", attempt,
                        "x-actor-retry-started-at", 0L)).build();
                assertTrue(strategy.execute(() -> {
                    throw new IOException();
                }, "queue", new byte[0], properties));
            }

            verify(publisher, times(39)).waitForConfirmsOrDie(5000);
        } finally {
            strategy.stop();
            TtlRequeueRetryScheduler.setMaxRetryQueues(TtlRequeueRetryScheduler.DEFAULT_MAX_RETRY_QUEUES);
        }
    }

    @Test
    void configuredDelayLimitRejectsPermanentlyRatherThanRequeuing() throws Exception {
        var config = new NoRetryConfig();
        config.setAsyncMaxDelayLevels(1);
        var connection = mock(RMQConnection.class);
        var topology = mock(Channel.class);
        var publisher = mock(Channel.class);
        when(connection.getRootObserver()).thenReturn(new TerminalRMQObserver());
        when(publisher.isOpen()).thenReturn(true);
        when(connection.newChannel()).thenReturn(topology, publisher, topology);
        var scheduler = new RetryStrategyFactory().createScheduler(config, new ActorConfig(), connection,
                "exchange", "queue");
        scheduler.start();
        scheduler.schedule("queue", new byte[0], new AMQP.BasicProperties(), new RetryState(2, 0), 100);

        assertThrows(RetrySchedulingRejectedException.class, () -> scheduler.schedule("queue", new byte[0],
                new AMQP.BasicProperties(), new RetryState(3, 0), 200));
        scheduler.stop();
    }

    @Test
    void nullModeFailsBeforeConstructingTransport() {
        var config = new NoRetryConfig();
        config.setWaitMode(null);

        var error = assertThrows(NullPointerException.class,
                () -> new RetryStrategyFactory().create(config, null, null, null, null));

        assertEquals("retryMode", error.getMessage());
    }

    @Test
    void asyncCreationFailsFastWhenLegacyCreateOverrideWouldBeBypassed() {
        var config = new NoRetryConfig();
        config.setWaitMode(WaitMode.ASYNC);
        var connection = mock(RMQConnection.class);
        var factory = new RetryStrategyFactory() {
            @Override
            public RetryStrategy create(RetryConfig actual) {
                return super.create(actual);
            }
        };

        var error = assertThrows(IllegalStateException.class,
                () -> factory.create(config, new ActorConfig(), connection, "exchange", "queue"));

        assertTrue(error.getMessage().contains("createStrategy(RetryConfig)"), error.getMessage());
        verifyNoInteractions(connection);
    }

    @Test
    void asyncStartFailsWhenPolicyNeedsMoreDelayLevelsThanConfigured() throws Exception {
        var config = new CountLimitedIncrementalWaitRetryConfig();
        config.setWaitMode(WaitMode.ASYNC);
        config.setMaxAttempts(40);
        var connection = mock(RMQConnection.class);
        when(connection.getRootObserver()).thenReturn(new TerminalRMQObserver());
        var strategy = new RetryStrategyFactory().create(config, new ActorConfig(), connection, "exchange", "queue");

        var error = assertThrows(IllegalStateException.class, strategy::start);

        assertTrue(error.getMessage().contains("39 distinct delays"), error.getMessage());
        verify(connection, never()).newChannel();
    }
}
