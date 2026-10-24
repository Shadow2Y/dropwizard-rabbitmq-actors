package io.appform.dropwizard.actors.retry.async;

import java.io.IOException;

/**
 * Signals that an async retry could not be handed off to the broker.
 * Distinguishes transport failure from message-processing failure so the
 * consumer requeues the delivery instead of treating it as a processing error.
 */
public class RetrySchedulingException extends IOException {

    public RetrySchedulingException(String message, Throwable cause) {
        super(message, cause);
    }
}
