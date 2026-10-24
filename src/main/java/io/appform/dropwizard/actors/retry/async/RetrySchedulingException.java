package io.appform.dropwizard.actors.retry.async;

import java.io.IOException;

/**
 * Signals that an async retry could not be handed off to the broker.
 * Propagates transport failure out of the consumer callback without applying
 * the message-processing exception policy or settling the delivery.
 */
public class RetrySchedulingException extends IOException {

    public RetrySchedulingException(String message, Throwable cause) {
        super(message, cause);
    }
}
