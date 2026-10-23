package io.appform.dropwizard.actors.retry.async;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.LongStringHelper;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.actor.QueueType;
import io.appform.dropwizard.actors.common.Constants;
import io.appform.dropwizard.actors.connectivity.PublishingChannel;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.observers.PublishMessageDetails;
import io.appform.dropwizard.actors.observers.PublishObserverContext;
import io.appform.dropwizard.actors.observers.RMQObserver;
import io.appform.dropwizard.actors.observers.TerminalRMQObserver;
import io.appform.dropwizard.actors.retry.RetryQueueScope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TtlRequeueRetrySchedulerTest {

    private static final String QUEUE = "queue_0";
    private static final String OTHER_QUEUE = "other-queue";
    private static final long STARTED_AT = 900L;
    private static final long NOW = 1_000L;

    private final RMQConnection connection = mock(RMQConnection.class);
    private final Channel topology = mock(Channel.class);
    private final Channel publisher = mock(Channel.class);
    private final byte[] body = new byte[]{1, 2};
    private final AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
            .messageId("message").deliveryMode(2).expiration("20")
            .headers(Map.of("custom", "value")).build();
    private TtlRequeueRetryScheduler scheduler;

    @BeforeEach
    void setUp() throws Exception {
        TtlRequeueRetryScheduler.resetRetryQueues();
        when(connection.getRootObserver()).thenReturn(new TerminalRMQObserver());
        when(connection.newChannel()).thenReturn(topology, publisher);
        when(publisher.isOpen()).thenReturn(true);
        doAnswer(invocation -> {
            var topologyChannel = invocation.<Channel>getArgument(0);
            topologyChannel.queueDeclare(invocation.getArgument(1), true, false, false, invocation.getArgument(4));
            topologyChannel.queueBind(invocation.getArgument(1), invocation.getArgument(3), invocation.getArgument(2));
            return null;
        }).when(connection).ensure(any(Channel.class), anyString(), anyString(), anyString(), any());
        scheduler = scheduler(ActorConfig.builder().exchange("exchange").queueType(QueueType.QUORUM)
                .priorityQueue(true).build(), Set.of("queue_0", "queue_1"), 5000, 32);
    }

    @AfterEach
    void tearDown() {
        TtlRequeueRetryScheduler.resetRetryQueues();
    }

    private TtlRequeueRetryScheduler scheduler(ActorConfig actorConfig, Set<String> consumedQueues,
                                               long confirmTimeoutMs, int maxDelayLevels) {
        return scheduler(actorConfig, "exchange", "queue", consumedQueues, confirmTimeoutMs, maxDelayLevels,
                RetryQueueScope.EXCHANGE);
    }

    private TtlRequeueRetryScheduler scheduler(ActorConfig actorConfig, String exchange, String queueName,
                                               Set<String> consumedQueues, long confirmTimeoutMs,
                                               int maxDelayLevels, RetryQueueScope scope) {
        return new TtlRequeueRetryScheduler(new TtlRequeueRetryScheduler.Context(connection, actorConfig, exchange,
                queueName, consumedQueues), new PublishingChannel(connection, confirmTimeoutMs), maxDelayLevels, scope);
    }

    private TtlRequeueRetryScheduler otherQuorumScheduler(String exchange) {
        return scheduler(ActorConfig.builder().exchange(exchange).queueType(QueueType.QUORUM).build(), exchange,
                "other", Set.of(OTHER_QUEUE), 5000, 32, RetryQueueScope.EXCHANGE);
    }

    private static String waiting(String queueType, long delayMs) {
        return TtlRequeueRetryScheduler.waitingQueueName("exchange", queueType, delayMs);
    }

    @Test
    void observesRetryDestinationAndPreservesOriginalAgeAndExpiry() throws Exception {
        var observer = mock(RMQObserver.class);
        doAnswer(invocation -> {
            var context = invocation.<PublishObserverContext>getArgument(0);
            var publish = invocation.<Function<PublishMessageDetails, Object>>getArgument(1);
            return publish.apply(PublishMessageDetails.builder()
                    .messageProperties(context.getMessageProperties()).build());
        }).when(observer).executePublish(any(), any());
        when(connection.getRootObserver()).thenReturn(observer);
        scheduler = scheduler(new ActorConfig(), Set.of(QUEUE), 1234, 32);
        var original = properties(Map.of(Constants.MESSAGE_PUBLISHED_TEXT, 800L,
                Constants.MESSAGE_EXPIRY_TEXT, 2000L));
        when(connection.newChannel()).thenReturn(topology, publisher, topology, publisher);
        scheduler.start();
        scheduler.schedule(QUEUE, body, original, new RetryState(2, STARTED_AT), 100);

        var context = ArgumentCaptor.forClass(PublishObserverContext.class);
        verify(observer).executePublish(context.capture(), any());
        assertEquals("queue_RETRY_100", context.getValue().getQueueName());
        var published = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(publisher).basicPublish(eq(waiting("classic", 100)), eq(QUEUE), eq(true),
                published.capture(), same(body));
        assertEquals(800L, published.getValue().getHeaders().get(Constants.MESSAGE_PUBLISHED_TEXT));
        assertEquals(2000L, published.getValue().getHeaders().get(Constants.MESSAGE_EXPIRY_TEXT));
        verify(publisher).waitForConfirmsOrDie(1234);
        scheduler.stop();
        scheduler.start();
        scheduler.schedule(QUEUE, body, original, new RetryState(3, STARTED_AT), 100);
        verify(connection, times(4)).newChannel();
        verify(topology, times(2)).exchangeDeclare(waiting("classic", 100), "fanout", true);
    }

    private AMQP.BasicProperties properties(Map<String, Object> headers) {
        return new AMQP.BasicProperties.Builder().headers(headers).build();
    }

    private Map<String, Object> retryHeaders(String queue, Object attempt, Object startedAt) {
        var headers = new LinkedHashMap<String, Object>();
        headers.put(TtlRequeueRetryScheduler.RETRY_QUEUE_HEADER, queue);
        if (attempt != null) {
            headers.put(TtlRequeueRetryScheduler.RETRY_ATTEMPT_HEADER, attempt);
        }
        if (startedAt != null) {
            headers.put(TtlRequeueRetryScheduler.RETRY_STARTED_AT_HEADER, startedAt);
        }
        return headers;
    }

    @Test
    void declaresOneWaitingQueuePerDelayThatDeadLettersBackThroughTheConsumersExchange() throws Exception {
        scheduler.start();

        for (long delay : Set.of(100L, 1000L)) {
            when(connection.newChannel()).thenReturn(topology, publisher, topology);
            scheduler.schedule("queue_0", body, properties,
                    new RetryState(2, STARTED_AT), delay);
            var waitingQueue = waiting("quorum", delay);
            verify(topology).exchangeDeclare(waitingQueue, "fanout", true);
            verify(topology).queueDeclare(waitingQueue, true, false, false,
                    Map.of("x-queue-type", "quorum", "x-message-ttl", delay, "x-dead-letter-exchange", "exchange"));
            verify(topology).queueBind(waitingQueue, waitingQueue, "");
        }
        verify(topology, never()).queueBind(anyString(), anyString(), eq("queue_1"));
        verify(topology, times(2)).close();
    }

    @Test
    void actorsWithTheSameQueueTypeAndDelayShareOneWaitingQueue() throws Exception {
        var other = otherQuorumScheduler("exchange");
        when(connection.newChannel()).thenReturn(topology, topology, publisher);
        scheduler.start();
        other.start();

        scheduler.prepare(Set.of(100L));
        other.prepare(Set.of(100L));
        other.schedule(OTHER_QUEUE, body, properties, new RetryState(2, STARTED_AT), 100);

        assertEquals(Set.of(waiting("quorum", 100)), TtlRequeueRetryScheduler.retryQueuesInUse());
        verify(publisher).basicPublish(eq(waiting("quorum", 100)), eq(OTHER_QUEUE), eq(true), any(), same(body));
    }

    @Test
    void consumersOfDifferentExchangesNeverShareWaitingQueues() throws Exception {
        var other = otherQuorumScheduler("other-exchange");
        var otherWaitingQueue = TtlRequeueRetryScheduler.waitingQueueName("other-exchange", "quorum", 100);
        when(connection.newChannel()).thenReturn(topology);
        scheduler.start();
        other.start();

        scheduler.prepare(Set.of(100L));
        other.prepare(Set.of(100L));

        assertEquals(Set.of(waiting("quorum", 100), otherWaitingQueue), TtlRequeueRetryScheduler.retryQueuesInUse());
        verify(topology).queueDeclare(otherWaitingQueue, true, false, false,
                Map.of("x-queue-type", "quorum", "x-message-ttl", 100L, "x-dead-letter-exchange", "other-exchange"));
    }

    @Test
    void queueScopeGivesEachConsumedQueueItsOwnWaitingQueue() throws Exception {
        scheduler = scheduler(ActorConfig.builder().exchange("exchange").build(), "exchange", "queue",
                Set.of("queue_0", "queue_1"), 5000, 32, RetryQueueScope.QUEUE);
        scheduler.start();

        scheduler.schedule("queue_1", body, properties, new RetryState(2, STARTED_AT), 100);

        verify(topology).exchangeDeclare("queue_RETRY_100", "fanout", true);
        verify(topology).queueDeclare("queue_RETRY_100", true, false, false,
                Map.of("x-queue-type", "classic", "x-message-ttl", 100L, "x-dead-letter-exchange", "exchange"));
        verify(publisher).basicPublish(eq("queue_RETRY_100"), eq("queue_1"), eq(true), any(), same(body));
        assertEquals(Set.of("queue_RETRY_100"), TtlRequeueRetryScheduler.retryQueuesInUse());
    }

    @Test
    void retryCopyDropsTheOriginalPublishDelaySoADelayedExchangeDoesNotDelayItAgain() throws Exception {
        scheduler.start();

        scheduler.schedule(QUEUE, body, properties(Map.of("x-delay", 5000, "custom", "value")),
                new RetryState(2, STARTED_AT), 100);

        var captured = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(publisher).basicPublish(anyString(), anyString(), anyBoolean(), captured.capture(), same(body));
        assertFalse(captured.getValue().getHeaders().containsKey("x-delay"));
        assertEquals("value", captured.getValue().getHeaders().get("custom"));
    }

    @Test
    void queueTypesDoNotShareWaitingQueues() throws Exception {
        var classic = scheduler(new ActorConfig(), Set.of(OTHER_QUEUE), 5000, 32);
        when(connection.newChannel()).thenReturn(topology);
        scheduler.start();
        classic.start();

        scheduler.prepare(Set.of(100L));
        classic.prepare(Set.of(100L));

        assertEquals(Set.of(waiting("quorum", 100), waiting("classic", 100)),
                TtlRequeueRetryScheduler.retryQueuesInUse());
    }

    @Test
    void processWideLimitFailsStartupOfTheSchedulerThatWouldExceedIt() throws Exception {
        TtlRequeueRetryScheduler.setMaxRetryQueues(2);
        var other = otherQuorumScheduler("exchange");
        when(connection.newChannel()).thenReturn(topology);
        scheduler.start();
        other.start();
        scheduler.prepare(Set.of(100L, 200L));

        var error = assertThrows(IllegalStateException.class, () -> other.prepare(Set.of(300L)));

        assertTrue(error.getMessage().contains("process-wide limit of 2"), error.getMessage());
        other.prepare(Set.of(100L));
        assertEquals(Set.of(waiting("quorum", 100), waiting("quorum", 200)),
                TtlRequeueRetryScheduler.retryQueuesInUse());
    }

    @Test
    void startupReservationIsAllOrNothing() throws Exception {
        TtlRequeueRetryScheduler.setMaxRetryQueues(2);
        var other = otherQuorumScheduler("exchange");
        when(connection.newChannel()).thenReturn(topology);
        scheduler.start();
        other.start();
        scheduler.prepare(Set.of(100L));

        assertThrows(IllegalStateException.class, () -> other.prepare(Set.of(200L, 300L)));

        assertEquals(Set.of(waiting("quorum", 100)), TtlRequeueRetryScheduler.retryQueuesInUse());
    }

    @Test
    void retryQueueLimitMustBePositiveAndCannotDropBelowQueuesInUse() throws Exception {
        when(connection.newChannel()).thenReturn(topology);
        scheduler.start();
        scheduler.prepare(Set.of(100L, 200L));

        assertThrows(IllegalArgumentException.class, () -> TtlRequeueRetryScheduler.setMaxRetryQueues(0));
        assertThrows(IllegalStateException.class, () -> TtlRequeueRetryScheduler.setMaxRetryQueues(1));
        TtlRequeueRetryScheduler.setMaxRetryQueues(2);
        assertEquals(2, TtlRequeueRetryScheduler.getMaxRetryQueues());
    }

    @Test
    void runtimeDelayBeyondProcessWideLimitIsRejectedPermanently() throws Exception {
        TtlRequeueRetryScheduler.setMaxRetryQueues(1);
        var other = otherQuorumScheduler("exchange");
        scheduler.start();
        other.start();
        scheduler.prepare(Set.of(100L));

        assertThrows(RetrySchedulingRejectedException.class,
                () -> other.schedule(OTHER_QUEUE, body, properties, new RetryState(2, STARTED_AT), 200));
        verify(publisher, never()).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    }

    @Test
    void stoppingReleasesWaitingQueuesForOtherSchedulers() throws Exception {
        TtlRequeueRetryScheduler.setMaxRetryQueues(1);
        var other = otherQuorumScheduler("exchange");
        when(connection.newChannel()).thenReturn(topology);
        scheduler.start();
        other.start();
        scheduler.prepare(Set.of(100L));

        scheduler.stop();
        other.prepare(Set.of(200L));

        assertEquals(Set.of(waiting("quorum", 200)), TtlRequeueRetryScheduler.retryQueuesInUse());
    }

    @Test
    void confirmsMandatoryPublishMergesRetryHeadersAndPreservesPayloadAndProperties() throws Exception {
        scheduler.start();
        scheduler.schedule("queue_1", body, properties, new RetryState(4, STARTED_AT), 100);

        var captured = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(publisher).confirmSelect();
        verify(publisher).basicPublish(eq(waiting("quorum", 100)), eq("queue_1"), eq(true),
                captured.capture(), same(body));
        verify(publisher).waitForConfirmsOrDie(5000);
        var published = captured.getValue();
        assertEquals(Map.of(TtlRequeueRetryScheduler.RETRY_QUEUE_HEADER, "queue_1",
                TtlRequeueRetryScheduler.RETRY_ATTEMPT_HEADER, 4L,
                TtlRequeueRetryScheduler.RETRY_STARTED_AT_HEADER, STARTED_AT,
                "custom", "value"), published.getHeaders());
        assertEquals("message", published.getMessageId());
        assertEquals(2, published.getDeliveryMode());
        assertNull(published.getExpiration());
    }

    @Test
    void scheduleMergesRetryHeadersWithoutMutatingOriginalProperties() throws Exception {
        scheduler.start();
        var originalHeaders = new HashMap<String, Object>(Map.of("custom", "value"));
        var originalProperties = properties(originalHeaders);

        scheduler.schedule("queue_0", body, originalProperties, new RetryState(2, STARTED_AT), 100);

        assertEquals(Map.of("custom", "value"), originalHeaders);
        var captured = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(publisher).basicPublish(anyString(), anyString(), anyBoolean(), captured.capture(), same(body));
        assertNotSame(originalProperties, captured.getValue());
        assertEquals(Map.of(TtlRequeueRetryScheduler.RETRY_QUEUE_HEADER, "queue_0",
                TtlRequeueRetryScheduler.RETRY_ATTEMPT_HEADER, 2L,
                TtlRequeueRetryScheduler.RETRY_STARTED_AT_HEADER, STARTED_AT,
                "custom", "value"), captured.getValue().getHeaders());
    }

    @Test
    void restoredStateSurvivesASchedulingRoundTrip() throws Exception {
        scheduler.start();
        scheduler.schedule(QUEUE, body, properties(Map.of("custom", "value")),
                new RetryState(3, STARTED_AT), 100);

        var captured = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
        verify(publisher).basicPublish(anyString(), anyString(), anyBoolean(), captured.capture(), same(body));
        var restored = scheduler.restoreState(QUEUE, captured.getValue(), NOW);

        assertEquals(new RetryState(3, STARTED_AT), restored);
    }

    @Test
    void declaresExactRuntimeDelayAndRejectsInvalidRequests() throws Exception {
        scheduler.start();

        scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), 5000);
        verify(publisher).basicPublish(eq(waiting("quorum", 5000)), eq("queue_0"), eq(true), any(), same(body));
        assertThrows(RetrySchedulingRejectedException.class,
                () -> scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), -1));
        assertThrows(RetrySchedulingRejectedException.class,
                () -> scheduler.schedule("another-queue", body, properties, new RetryState(2, STARTED_AT), 100));
        assertThrows(RetrySchedulingRejectedException.class,
                () -> scheduler.schedule("queue_0", null, properties, new RetryState(2, STARTED_AT), 100));
    }

    @Test
    void prepareDeclaresEveryPlannedDelayOnOneChannel() throws Exception {
        scheduler.start();
        scheduler.prepare(Set.of(100L, 1000L));

        verify(connection, times(1)).newChannel();
        verify(topology).exchangeDeclare(waiting("quorum", 100), "fanout", true);
        verify(topology).exchangeDeclare(waiting("quorum", 1000), "fanout", true);
        verify(topology).close();

        scheduler.schedule(QUEUE, body, properties, new RetryState(2, STARTED_AT), 1000);

        verify(topology, times(1)).exchangeDeclare(waiting("quorum", 1000), "fanout", true);
        verify(publisher).basicPublish(eq(waiting("quorum", 1000)), eq(QUEUE), eq(true), any(), same(body));
    }

    @Test
    void prepareFailsWhenPolicyNeedsMoreDelayLevelsThanAllowed() throws Exception {
        scheduler = scheduler(ActorConfig.builder().exchange("exchange").build(), Set.of(QUEUE), 5000, 2);
        scheduler.start();

        var error = assertThrows(IllegalStateException.class, () -> scheduler.prepare(Set.of(100L, 200L, 300L)));

        assertTrue(error.getMessage().contains("asyncMaxDelayLevels is 2"), error.getMessage());
        verify(connection, never()).newChannel();
    }

    @Test
    void failedHandoffRedeclaresTopologyOnNextSchedule() throws Exception {
        scheduler.start();
        scheduler.schedule(QUEUE, body, properties, new RetryState(2, STARTED_AT), 100);
        doThrow(channelClose(AMQP.NOT_FOUND, "NOT_FOUND - no exchange"))
                .when(publisher).waitForConfirmsOrDie(5000);

        assertThrows(ShutdownSignalException.class,
                () -> scheduler.schedule(QUEUE, body, properties, new RetryState(2, STARTED_AT), 100));

        var redeclared = mock(Channel.class);
        var nextPublisher = mock(Channel.class);
        when(nextPublisher.isOpen()).thenReturn(true);
        when(connection.newChannel()).thenReturn(redeclared, nextPublisher);
        scheduler.schedule(QUEUE, body, properties, new RetryState(2, STARTED_AT), 100);

        verify(topology, times(1)).exchangeDeclare(waiting("quorum", 100), "fanout", true);
        verify(redeclared).exchangeDeclare(waiting("quorum", 100), "fanout", true);
        verify(nextPublisher).basicPublish(eq(waiting("quorum", 100)), eq(QUEUE), eq(true), any(), same(body));
    }

    @Test
    void brokerRefusalsAreRejectedPermanently() throws Exception {
        var inequivalentArguments = new IOException("declare failed",
                channelClose(AMQP.PRECONDITION_FAILED, "PRECONDITION_FAILED - inequivalent arg 'x-queue-type'"));
        doThrow(inequivalentArguments).when(topology).exchangeDeclare(waiting("quorum", 100), "fanout", true);
        var accessRefused = channelClose(AMQP.ACCESS_REFUSED, "ACCESS_REFUSED - write access denied");
        doThrow(accessRefused).when(publisher).waitForConfirmsOrDie(5000);
        scheduler.start();

        var declareError = assertThrows(RetrySchedulingRejectedException.class,
                () -> scheduler.schedule(QUEUE, body, properties, new RetryState(2, STARTED_AT), 100));
        when(connection.newChannel()).thenReturn(topology, publisher);
        var publishError = assertThrows(RetrySchedulingRejectedException.class,
                () -> scheduler.schedule(QUEUE, body, properties, new RetryState(2, STARTED_AT), 200));

        assertSame(inequivalentArguments, declareError.getCause());
        assertSame(accessRefused, publishError.getCause());
    }

    private static ShutdownSignalException channelClose(int replyCode, String replyText) {
        return new ShutdownSignalException(false, false,
                new AMQP.Channel.Close.Builder().replyCode(replyCode).replyText(replyText).build(), null);
    }

    @Test
    void returnedMessagesFailAndDiscardChannel() throws Exception {
        var callback = new AtomicReference<ReturnCallback>();
        when(publisher.addReturnListener(any(ReturnCallback.class))).thenAnswer(invocation -> {
            callback.set(invocation.getArgument(0));
            return null;
        });
        doAnswer(invocation -> {
            callback.get().handle(null);
            return null;
        }).when(publisher).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
        scheduler.start();

        assertThrows(IOException.class,
                () -> scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), 100));
        verify(publisher).close();
    }

    @Test
    void confirmTimeoutDiscardsChannelAndNextHandoffOpensAnother() throws Exception {
        doThrow(new TimeoutException()).when(publisher).waitForConfirmsOrDie(5000);
        scheduler.start();
        assertThrows(TimeoutException.class,
                () -> scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), 100));
        verify(publisher).close();
        var next = mock(Channel.class);
        when(next.isOpen()).thenReturn(true);
        when(connection.newChannel()).thenReturn(next);

        scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), 100);

        verify(next).confirmSelect();
        verify(next).waitForConfirmsOrDie(5000);
    }

    @Test
    void boundsRuntimeDelayLevelsWithoutShorteningRequests() throws Exception {
        scheduler = scheduler(ActorConfig.builder().exchange("exchange").build(), Set.of("queue_0"), 5000, 1);
        scheduler.start();
        scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), 100);

        assertThrows(RetrySchedulingRejectedException.class,
                () -> scheduler.schedule("queue_0", body, properties, new RetryState(3, STARTED_AT), 200));
        verify(publisher, times(1)).basicPublish(anyString(), anyString(), anyBoolean(), any(), any());
    }

    @Test
    void reusesChannelAndRejectsSchedulingOutsideLifecycle() throws Exception {
        assertThrows(IllegalStateException.class,
                () -> scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), 100));
        when(connection.newChannel()).thenReturn(topology, publisher, topology);
        scheduler.start();
        scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), 100);
        scheduler.schedule("queue_1", body, properties, new RetryState(2, STARTED_AT), 100);
        verify(connection, times(2)).newChannel();
        verify(topology, times(1)).queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), any());

        scheduler.stop();

        verify(publisher).close();
        assertThrows(IllegalStateException.class,
                () -> scheduler.schedule("queue_0", body, properties, new RetryState(2, STARTED_AT), 100));
    }

    @Test
    void freshDeliveryWithoutRetryHeadersStartsAtAttemptOne() {
        assertEquals(new RetryState(1, NOW), scheduler.restoreState(QUEUE, new AMQP.BasicProperties(), NOW));
        assertEquals(new RetryState(1, NOW), scheduler.restoreState(QUEUE, null, NOW));
        assertEquals(new RetryState(1, NOW),
                scheduler.restoreState(QUEUE, properties(Map.of("custom", "value")), NOW));
    }

    @Test
    void replayStartsFreshOnlyWhenAllRetryHeadersAreRemoved() {
        var headers = retryHeaders(QUEUE, 7L, STARTED_AT);
        headers.put("custom", "value");
        assertEquals(new RetryState(7, STARTED_AT), scheduler.restoreState(QUEUE, properties(headers), NOW));
        var replayHeaders = new HashMap<>(headers);
        replayHeaders.remove(TtlRequeueRetryScheduler.RETRY_QUEUE_HEADER);
        replayHeaders.remove(TtlRequeueRetryScheduler.RETRY_ATTEMPT_HEADER);
        replayHeaders.remove(TtlRequeueRetryScheduler.RETRY_STARTED_AT_HEADER);

        assertEquals(new RetryState(1, NOW), scheduler.restoreState(QUEUE, properties(replayHeaders), NOW));
        assertEquals("value", replayHeaders.get("custom"));
        assertEquals(7L, headers.get(TtlRequeueRetryScheduler.RETRY_ATTEMPT_HEADER));
    }

    @Test
    void futureStartedAtPreservesRetryProgressAcrossClockSkew() {
        assertEquals(new RetryState(2, NOW + 500),
                scheduler.restoreState(QUEUE, properties(retryHeaders(QUEUE, 2L, NOW + 500)), NOW));
    }

    @Test
    void retryHeadersOnTheirQueueRestoreThePersistedState() {
        assertEquals(new RetryState(2, STARTED_AT),
                scheduler.restoreState(QUEUE, properties(retryHeaders(QUEUE, 2L, STARTED_AT)), NOW));
    }

    @Test
    void integerSubtypesInHeadersAreAccepted() {
        assertEquals(new RetryState(3, STARTED_AT),
                scheduler.restoreState(QUEUE, properties(retryHeaders(QUEUE, (byte) 3, (short) STARTED_AT)), NOW));
        assertEquals(new RetryState(3, STARTED_AT),
                scheduler.restoreState(QUEUE, properties(retryHeaders(QUEUE, 3, (int) STARTED_AT)), NOW));
    }

    @Test
    void amqpLongStringQueueHeaderIsAcceptedViaToString() {
        var headers = retryHeaders(QUEUE, 2L, STARTED_AT);
        headers.put(TtlRequeueRetryScheduler.RETRY_QUEUE_HEADER,
                LongStringHelper.asLongString(QUEUE));
        assertEquals(new RetryState(2, STARTED_AT), scheduler.restoreState(QUEUE, properties(headers), NOW));
    }

    @Test
    void retryHeadersScopedToAnotherQueueRestartAtAttemptOne() {
        var headers = retryHeaders(OTHER_QUEUE, 7L, 123L);
        assertEquals(new RetryState(1, NOW), scheduler.restoreState(QUEUE, properties(headers), NOW));
    }

    @Test
    void malformedRetryMetadataFailsRestoreState() {
        var cases = new LinkedHashMap<String, Map<String, Object>>();
        cases.put("zero attempt count", retryHeaders(QUEUE, 0L, 900L));
        cases.put("negative attempt count", retryHeaders(QUEUE, -3L, 900L));
        cases.put("attempt count at int limit", retryHeaders(QUEUE, Integer.MAX_VALUE, 900L));
        cases.put("non numeric attempt count", retryHeaders(QUEUE, "two", 900L));
        cases.put("fractional attempt count", retryHeaders(QUEUE, 2.5d, 900L));
        cases.put("float attempt count", retryHeaders(QUEUE, 2.0f, 900L));
        cases.put("overflowing attempt count type", retryHeaders(QUEUE, BigInteger.valueOf(2), 900L));
        cases.put("negative started at", retryHeaders(QUEUE, 2L, -1L));
        cases.put("non numeric started at", retryHeaders(QUEUE, 2L, "yesterday"));
        cases.put("fractional started at", retryHeaders(QUEUE, 2L, 900.5d));
        cases.put("overflowing started at type", retryHeaders(QUEUE, 2L, BigInteger.valueOf(900)));
        cases.put("attempt count without queue scoping",
                Map.of(TtlRequeueRetryScheduler.RETRY_ATTEMPT_HEADER, 2L,
                        TtlRequeueRetryScheduler.RETRY_STARTED_AT_HEADER, 900L));
        cases.put("started at without queue scoping",
                Map.of(TtlRequeueRetryScheduler.RETRY_STARTED_AT_HEADER, 900L));

        cases.forEach((name, headers) -> {
            var error = assertThrows(IllegalStateException.class,
                    () -> scheduler.restoreState(QUEUE, properties(headers), NOW), name);
            assertTrue(error.getMessage().contains("Malformed actor retry metadata"), name);
        });
    }
}
