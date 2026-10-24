package io.appform.dropwizard.actors.retry.async;

import com.rabbitmq.client.AMQP;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.actor.QueueType;
import io.appform.dropwizard.actors.base.utils.NamingUtils;
import io.appform.dropwizard.actors.connectivity.PublishingChannel;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.retry.RetryPolicy;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TtlRequeueRetryScheduler implements AsyncRetryScheduler {

    static final String RETRY_QUEUE_HEADER = "x-actor-retry-queue";
    static final String RETRY_ATTEMPT_HEADER = "x-actor-retry-attempt";
    static final String RETRY_STARTED_AT_HEADER = "x-actor-retry-started-at";
    public static final int DEFAULT_MAX_RETRY_QUEUES = 20;
    private static final String DELAY_HEADER = "x-delay";

    private final String queuePrefix;
    private final ActorConfig actorConfig;
    private final RMQConnection connection;
    private final PublishingChannel publisher;
    private final RetryPolicy<Boolean> retryPolicy;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public TtlRequeueRetryScheduler(@NonNull RMQConnection connection, @NonNull RetryPolicy<Boolean> retryPolicy,
        @NonNull ActorConfig actorConfig, @NonNull String retryPrefix, @NonNull PublishingChannel publisher) {
        this.connection = connection;
        this.retryPolicy = retryPolicy;
        this.actorConfig = actorConfig;
        this.queuePrefix = retryPrefix;
        this.publisher = publisher;
    }

    @Override
    public synchronized void start() throws Exception {
        if (running.get()) {
            return;
        }
        publisher.start();
        declareTopology(AsyncRetryer.getPossibleDelays(retryPolicy, 10, true));
        running.set(true);
    }

    @Override
    public RetryState restoreState(String queueName, AMQP.BasicProperties properties, long now) {
        Objects.requireNonNull(queueName, "queueName");
        var headers = properties == null || properties.getHeaders() == null
                ? Map.<String, Object>of() : properties.getHeaders();
        var retryQueue = headers.get(RETRY_QUEUE_HEADER);
        var attemptHeader = headers.get(RETRY_ATTEMPT_HEADER);
        var startedAtHeader = headers.get(RETRY_STARTED_AT_HEADER);
        if (retryQueue == null && attemptHeader == null && startedAtHeader == null) {
            return new RetryState(1, now);
        }
        if (retryQueue == null) {
            throw malformedMetadata("Retry metadata is missing the queue header");
        }
        if (!queueName.equals(retryQueue.toString())) {
            return new RetryState(1, now);
        }
        var attemptCount = headerNumber(attemptHeader);
        var startedAt = headerNumber(startedAtHeader);
        if (attemptCount == null || attemptCount <= 0 || attemptCount >= Integer.MAX_VALUE) {
            throw malformedMetadata("Attempt count must be a positive integer below " + Integer.MAX_VALUE);
        }
        if (startedAt == null || startedAt < 0) {
            throw malformedMetadata("Started-at must be a nonnegative timestamp");
        }
        return new RetryState(attemptCount, startedAt);
    }

    @Override
    public synchronized void schedule(String queueName, byte[] body, AMQP.BasicProperties properties,
                                       RetryState nextState, long delayMs) throws Exception {
        if (!running.get()) {
            throw new RetrySchedulingException("Retry scheduler is not running", null);
        }
        try {
            publisher.publish(waitingQueue(delayMs), actorConfig.getExchange(),
                    queueName, mergedProperties(properties, queueName, nextState).builder().expiration(null).build(),
                    body);
        } catch (Exception error) {
            throw new RetrySchedulingException("TTL Retry Requeue failed", error);
        }
        log.debug("Async retry published: queue: {}, delayMs: {}", queueName, delayMs);
    }

    @Override
    public synchronized void stop() throws Exception {
        running.set(false);
        publisher.close();
    }

    protected String waitingQueue(long delayMs) {
        return NamingUtils.prefixWithNamespace(queuePrefix + "_RETRY_DLQ_" + delayMs);
    }

    private static Long headerNumber(Object value) {
        if (value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        return null;
    }

    private static IllegalStateException malformedMetadata(String detail) {
        return new IllegalStateException("Malformed actor retry metadata: " + detail);
    }

    private void declareTopology(Collection<Long> delays) throws Exception {
        try (var topology = connection.newChannel()) {
            for (var delay : delays) {
                var waitingQueue = waitingQueue(delay);
                var arguments = new HashMap<String, Object>();
                arguments.put("x-message-ttl", delay);
                arguments.put(QueueType.X_QUEUE_TYPE, actorConfig.getQueueType());
                arguments.put("x-dead-letter-exchange", actorConfig.getExchange());
                topology.exchangeDeclare(waitingQueue, "fanout", true);
                connection.ensure(topology, waitingQueue, "", waitingQueue, arguments);
                log.info("Declared async retry waiting queue: {}, delayMs: {}, dead-letters to: {}",
                    waitingQueue, delay, actorConfig.getExchange());
            }
        }
    }

    private static AMQP.BasicProperties mergedProperties(AMQP.BasicProperties properties,
                                                          String queueName, RetryState nextState) {
        Map<String, Object> merged = new HashMap<>();
        if (properties.getHeaders() != null) {
            merged.putAll(properties.getHeaders());
        }
        merged.remove(DELAY_HEADER);
        merged.put(RETRY_QUEUE_HEADER, queueName);
        merged.put(RETRY_ATTEMPT_HEADER, nextState.attemptNumber());
        merged.put(RETRY_STARTED_AT_HEADER, nextState.startedAt());
        return properties.builder().headers(merged).build();
    }

}
