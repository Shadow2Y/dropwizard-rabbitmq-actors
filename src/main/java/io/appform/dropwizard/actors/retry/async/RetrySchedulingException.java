package io.appform.dropwizard.actors.retry.async;

/**
 * Signals that an async retry schedule could not be completed.
 */
public class RetrySchedulingException extends Exception {

    RetrySchedulingException(String message, Throwable cause) {
        super(message, cause);
    }
}
