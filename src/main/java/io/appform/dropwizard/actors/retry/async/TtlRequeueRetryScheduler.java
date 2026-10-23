package io.appform.dropwizard.actors.retry.async;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import io.appform.dropwizard.actors.actor.ActorConfig;
import io.appform.dropwizard.actors.actor.QueueType;
import io.appform.dropwizard.actors.actor.QueueTypeVisitorImpl;
import io.appform.dropwizard.actors.base.utils.NamingUtils;
import io.appform.dropwizard.actors.connectivity.PublishingChannel;
import io.appform.dropwizard.actors.connectivity.RMQConnection;
import io.appform.dropwizard.actors.retry.RetryQueueScope;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Fixed-TTL waiting queues, one per delay and per exchange or consumed queue (see {@link RetryQueueScope}). A retry
 * is published through the waiting queue's fanout exchange with the consumed queue as routing key, and dead-letters
 * back through the consumer's exchange to that queue.
 */
@Slf4j
public final class TtlRequeueRetryScheduler implements AsyncRetryScheduler {

    static final String RETRY_QUEUE_HEADER = "x-actor-retry-queue";
    static final String RETRY_ATTEMPT_HEADER = "x-actor-retry-attempt";
    static final String RETRY_STARTED_AT_HEADER = "x-actor-retry-started-at";
    public static final int DEFAULT_MAX_RETRY_QUEUES = 32;
    private static final String DELAY_HEADER = "x-delay";
    private static final Map<String, Integer> RETRY_QUEUE_USERS = new HashMap<>();
    private static int maxRetryQueues = DEFAULT_MAX_RETRY_QUEUES;

    public record Context(RMQConnection connection, ActorConfig actorConfig,
                          String exchange, String queueName, Set<String> consumedQueues) {
        public Context {
            Objects.requireNonNull(connection, "connection");
            Objects.requireNonNull(actorConfig, "actorConfig");
            Objects.requireNonNull(exchange, "exchange");
            Objects.requireNonNull(queueName, "queueName");
            if (consumedQueues == null || consumedQueues.isEmpty()) {
                throw new IllegalArgumentException("consumedQueues must be nonempty");
            }
            consumedQueues = Set.copyOf(consumedQueues);
        }
    }

    private final Context context;
    private final String queueType;
    private final int maxDelayLevels;
    private final PublishingChannel publisher;
    private final RetryQueueScope scope;
    private final Set<Long> delayLevels = new HashSet<>();
    private final Set<Long> declaredDelays = new HashSet<>();
    private boolean running;

    public TtlRequeueRetryScheduler(Context context, long confirmTimeoutMs, int maxDelayLevels) {
        this(context, new PublishingChannel(Objects.requireNonNull(context, "context").connection(),
                confirmTimeoutMs), maxDelayLevels, RetryQueueScope.EXCHANGE);
    }

    public TtlRequeueRetryScheduler(Context context, PublishingChannel publisher, int maxDelayLevels,
                                    RetryQueueScope scope) {
        this.context = Objects.requireNonNull(context, "context");
        if (maxDelayLevels < 1) {
            throw new IllegalArgumentException("Maximum delay levels must be positive");
        }
        this.maxDelayLevels = maxDelayLevels;
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.queueType = String.valueOf(context.actorConfig().getQueueType()
                .handleConfig(new QueueTypeVisitorImpl(context.actorConfig()))
                .get(QueueType.X_QUEUE_TYPE));
    }

    /**
     * Name of the waiting queue, and of the fanout exchange in front of it, shared by the consumers of one exchange
     * with the same queue type and delay.
     */
    public static String waitingQueueName(String exchange, String queueType, long delayMs) {
        return NamingUtils.prefixWithNamespace(String.format("%s_RETRY_%s_%d", exchange, queueType, delayMs));
    }

    /**
     * Caps the distinct waiting queues used by all schedulers in the process; call before actors start.
     */
    public static synchronized void setMaxRetryQueues(int maxQueues) {
        if (maxQueues < 1) {
            throw new IllegalArgumentException("Maximum retry queues must be positive");
        }
        if (maxQueues < RETRY_QUEUE_USERS.size()) {
            throw new IllegalStateException(String.format(
                    "Cannot lower the retry queue limit to %d while %d retry queues are in use",
                    maxQueues, RETRY_QUEUE_USERS.size()));
        }
        maxRetryQueues = maxQueues;
    }

    public static synchronized int getMaxRetryQueues() {
        return maxRetryQueues;
    }

    static synchronized Set<String> retryQueuesInUse() {
        return Set.copyOf(RETRY_QUEUE_USERS.keySet());
    }

    static synchronized void resetRetryQueues() {
        RETRY_QUEUE_USERS.clear();
        maxRetryQueues = DEFAULT_MAX_RETRY_QUEUES;
    }

    private static synchronized boolean reserveRetryQueues(Collection<String> queues) {
        var requested = Set.copyOf(queues);
        var added = requested.stream().filter(queue -> !RETRY_QUEUE_USERS.containsKey(queue)).count();
        if (RETRY_QUEUE_USERS.size() + added > maxRetryQueues) {
            return false;
        }
        requested.forEach(queue -> RETRY_QUEUE_USERS.merge(queue, 1, Integer::sum));
        return true;
    }

    private static synchronized void releaseRetryQueues(Collection<String> queues) {
        Set.copyOf(queues).forEach(queue -> RETRY_QUEUE_USERS.computeIfPresent(queue,
                (name, users) -> users == 1 ? null : users - 1));
    }

    @Override
    public synchronized void start() throws Exception {
        if (running) {
            throw new IllegalStateException("Retry scheduler is already running");
        }
        publisher.start();
        running = true;
    }

    @Override
    public synchronized void prepare(Set<Long> delaysMs) throws Exception {
        if (!running) {
            throw new IllegalStateException("Retry scheduler is not running");
        }
        if (delaysMs.size() > maxDelayLevels) {
            throw new IllegalStateException(String.format(
                    "Retry policy for queue %s can request %d distinct delays but asyncMaxDelayLevels is %d",
                    context.queueName(), delaysMs.size(), maxDelayLevels));
        }
        var newLevels = delaysMs.stream().filter(delay -> !delayLevels.contains(delay)).toList();
        if (!reserveRetryQueues(waitingQueues(newLevels))) {
            throw new IllegalStateException(String.format(
                    "Retry policy for queue %s needs retry queues %s but %d of the process-wide limit of %d are "
                            + "in use; raise it with TtlRequeueRetryScheduler.setMaxRetryQueues",
                    context.queueName(), waitingQueues(newLevels), retryQueuesInUse().size(),
                    getMaxRetryQueues()));
        }
        delayLevels.addAll(newLevels);
        declareTopology(delaysMs);
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

    private static Long headerNumber(Object value) {
        if (value instanceof Byte || value instanceof Short
                || value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        return null;
    }

    private static IllegalStateException malformedMetadata(String detail) {
        return new IllegalStateException("Malformed actor retry metadata: " + detail);
    }

    private void reserve(long delayMs) throws RetrySchedulingRejectedException {
        if (delayLevels.contains(delayMs)) {
            return;
        }
        if (delayLevels.size() >= maxDelayLevels) {
            throw new RetrySchedulingRejectedException(String.format(
                    "Retry policy for queue %s exceeded asyncMaxDelayLevels of %d",
                    context.queueName(), maxDelayLevels));
        }
        var waitingQueue = waitingQueue(delayMs);
        if (!reserveRetryQueues(List.of(waitingQueue))) {
            throw new RetrySchedulingRejectedException(String.format(
                    "Process-wide limit of %d retry queues reached; cannot add %s",
                    getMaxRetryQueues(), waitingQueue));
        }
        delayLevels.add(delayMs);
    }

    private void declareTopology(Collection<Long> delays) throws Exception {
        var missing = delays.stream()
                .filter(delay -> !declaredDelays.contains(delay))
                .distinct()
                .toList();
        if (missing.isEmpty()) {
            return;
        }
        var connection = context.connection();
        try (var topology = connection.newChannel()) {
            for (var delay : missing) {
                declareWaitingQueue(connection, topology, delay);
                declaredDelays.add(delay);
            }
        }
    }

    private void declareWaitingQueue(RMQConnection connection, Channel topology, long delay) throws Exception {
        var waitingQueue = waitingQueue(delay);
        var arguments = new HashMap<String, Object>();
        arguments.put(QueueType.X_QUEUE_TYPE, queueType);
        arguments.put("x-message-ttl", delay);
        arguments.put("x-dead-letter-exchange", context.exchange());
        topology.exchangeDeclare(waitingQueue, "fanout", true);
        connection.ensure(topology, waitingQueue, "", waitingQueue, arguments);
        log.info("Declared async retry waiting queue: {}, delayMs: {}, dead-letters to: {}",
                waitingQueue, delay, context.exchange());
    }

    @Override
    public synchronized void schedule(String queueName, byte[] body, AMQP.BasicProperties properties,
                                       RetryState nextState, long delayMs) throws Exception {
        if (!running) {
            throw new IllegalStateException("Retry scheduler is not running");
        }
        if (!context.consumedQueues().contains(queueName) || delayMs < 0
                || body == null || properties == null || nextState == null) {
            throw new RetrySchedulingRejectedException(
                    "Invalid retry request for queue: " + queueName + ", delayMs: " + delayMs);
        }
        reserve(delayMs);
        try {
            declareTopology(List.of(delayMs));
            publisher.publish(retryName(context.queueName(), delayMs), waitingQueue(delayMs),
                    queueName, mergedProperties(properties, queueName, nextState).builder().expiration(null).build(),
                    body);
        } catch (Exception error) {
            declaredDelays.remove(delayMs);
            throw error;
        }
        log.debug("Async retry published: queue: {}, delayMs: {}", queueName, delayMs);
    }

    @Override
    public synchronized void stop() throws Exception {
        running = false;
        releaseRetryQueues(waitingQueues(delayLevels));
        delayLevels.clear();
        declaredDelays.clear();
        publisher.close();
    }

    private String waitingQueue(long delayMs) {
        return switch (scope) {
            case EXCHANGE -> waitingQueueName(context.exchange(), queueType, delayMs);
            case QUEUE -> retryName(context.queueName(), delayMs);
        };
    }

    private List<String> waitingQueues(Collection<Long> delays) {
        return delays.stream().map(this::waitingQueue).toList();
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

    private static String retryName(String name, long delayMs) {
        return name + "_RETRY_" + delayMs;
    }
}
