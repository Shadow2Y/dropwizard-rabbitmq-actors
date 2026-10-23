/*
 * Copyright (c) 2019 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.appform.dropwizard.actors.retry;

import com.github.rholder.retry.Retryer;
import com.rabbitmq.client.AMQP;
import io.appform.dropwizard.actors.retry.async.ActorRetryer;
import lombok.Getter;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Defines retry policy, with synchronous execution unless the factory binds an executor.
 */
public abstract class RetryStrategy {

    interface SyncRetryer extends ActorRetryer {
    }

    /**
     * Null for strategies built from a legacy {@link Retryer}.
     */
    @Getter
    private final ActorRetryerBuilder.Policy<Boolean> policy;
    private final RetryMode retryMode;
    private final SyncRetryer syncRetryer;
    private volatile ActorRetryer actorRetryer;

    /**
     * @deprecated supports only {@link RetryMode#SYNC}; pass an {@link ActorRetryerBuilder.Policy} instead.
     */
    @Deprecated
    protected RetryStrategy(Retryer<Boolean> retryer) {
        this.policy = null;
        this.retryMode = RetryMode.SYNC;
        this.syncRetryer = syncRetryer(Objects.requireNonNull(retryer, "retryer"));
    }

    protected RetryStrategy(ActorRetryerBuilder.Policy<Boolean> policy) {
        this(policy, RetryMode.SYNC);
    }

    protected RetryStrategy(ActorRetryerBuilder.Policy<Boolean> policy, RetryMode retryMode) {
        this.policy = Objects.requireNonNull(policy, "policy");
        this.retryMode = Objects.requireNonNull(retryMode, "retryMode");
        this.syncRetryer = retryMode == RetryMode.SYNC ? syncRetryer(policy.buildSyncRetryer()) : null;
    }

    static SyncRetryer syncRetryer(Retryer<Boolean> retryer) {
        return (callable, queueName, body, properties) -> retryer.call(callable);
    }

    final void setActorRetryer(ActorRetryer actorRetryer) {
        Objects.requireNonNull(actorRetryer, "actorRetryer");
        if (this.actorRetryer != null) {
            throw new IllegalStateException("Retry executor has already been bound");
        }
        this.actorRetryer = actorRetryer;
    }

    public boolean execute(Callable<Boolean> callable) throws Exception {
        if (retryMode == RetryMode.ASYNC) {
            throw new UnsupportedOperationException(
                    "ASYNC retries need the delivery; use execute(callable, queueName, body, properties)");
        }
        return retryer().call(callable, null, null, null);
    }

    public boolean execute(Callable<Boolean> callable, String queueName, byte[] body,
                           AMQP.BasicProperties properties) throws Exception {
        var retryer = retryer();
        return retryMode == RetryMode.SYNC && retryer instanceof SyncRetryer
                ? execute(callable)
                : retryer.call(callable, queueName, body, properties);
    }

    public void start() throws Exception {
        retryer().start();
    }

    public void stop() throws Exception {
        retryer().stop();
    }

    private ActorRetryer retryer() {
        var bound = actorRetryer;
        if (bound != null) {
            return bound;
        }
        if (syncRetryer == null) {
            throw new IllegalStateException("Async retry executor must be bound by RetryStrategyFactory");
        }
        return syncRetryer;
    }

}
