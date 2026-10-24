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

import com.rabbitmq.client.AMQP;
import io.appform.dropwizard.actors.retry.async.ActorRetryer;
import java.util.Objects;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;

/**
 * Defines retry policy, with synchronous execution unless the factory binds an executor.
 */
public abstract class RetryStrategy {

    @Setter
    private ActorRetryer actorRetryer;
    @Getter @Nullable
    private final RetryPolicy<Boolean> retryPolicy;

    protected RetryStrategy(ActorRetryer actorRetryer, RetryPolicy<Boolean> retryPolicy) {
        this.actorRetryer = Objects.requireNonNull(actorRetryer, "actorRetryer");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy");
    }

    protected RetryStrategy(RetryPolicy<Boolean> policy) {
        this(
            RetryStrategyFactory.syncRetryer(policy.buildRetryer()),
            policy);
    }

    /**
     * @deprecated
     * Use the other execute instead
     */
    @Deprecated
    public boolean execute(Callable<Boolean> callable) throws Exception {
        return actorRetryer.call(callable, null, null, null);
    }

    public boolean execute(Callable<Boolean> callable, String queueName, byte[] body, AMQP.BasicProperties properties) throws Exception {
        return actorRetryer.call(callable, queueName, body, properties);
    }

    public void start() throws Exception {
        actorRetryer.start();
    }

    public void stop() throws Exception {
        actorRetryer.stop();
    }

}
