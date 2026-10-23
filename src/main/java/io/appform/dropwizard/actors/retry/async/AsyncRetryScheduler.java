package io.appform.dropwizard.actors.retry.async;

import com.rabbitmq.client.AMQP;

import java.util.Set;

public interface AsyncRetryScheduler {

    default void start() throws Exception {
    }

    /**
     * Receives, after {@link #start()}, every delay the retry policy can request. Implementations may provision
     * them eagerly and should throw on misconfiguration so the consumer fails to start.
     */
    default void prepare(Set<Long> delaysMs) throws Exception {
    }

    default void stop() throws Exception {
    }

    /**
     * Restores the retry progress carried by a delivery, or an initial state
     * for a fresh delivery. Throws {@link IllegalStateException} on malformed
     * persisted metadata; the delivery then follows the consumer's processing-error
     * policy without running the handler.
     */
    RetryState restoreState(String queueName, AMQP.BasicProperties properties, long now);

    /**
     * Completes the retry handoff before returning. Permanent rejections should throw
     * {@link RetrySchedulingRejectedException}; other exceptions requeue the original delivery.
     */
    void schedule(String queueName, byte[] body, AMQP.BasicProperties properties,
                  RetryState nextState, long delayMs) throws Exception;

}
