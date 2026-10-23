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

package io.appform.dropwizard.actors.retry.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.appform.dropwizard.actors.retry.RetryMode;
import io.appform.dropwizard.actors.retry.RetryQueueScope;
import io.appform.dropwizard.actors.retry.RetryType;
import io.dropwizard.util.Duration;
import io.dropwizard.validation.MinDuration;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configure a retry strategy
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "COUNT_LIMITED_EXPONENTIAL_BACKOFF", value =
                CountLimitedExponentialWaitRetryConfig.class),
        @JsonSubTypes.Type(name = "COUNT_LIMITED_FIXED_WAIT", value = CountLimitedFixedWaitRetryConfig.class),
        @JsonSubTypes.Type(name = "COUNT_LIMITED_INCREMENTAL_WAIT", value =
                CountLimitedIncrementalWaitRetryConfig.class),
        @JsonSubTypes.Type(name = "NO_RETRY", value = NoRetryConfig.class),
        @JsonSubTypes.Type(name = "TIME_LIMITED_EXPONENTIAL_BACKOFF", value =
                TimeLimitedExponentialWaitRetryConfig.class),
        @JsonSubTypes.Type(name = "TIME_LIMITED_FIXED_WAIT", value = TimeLimitedFixedWaitRetryConfig.class),
        @JsonSubTypes.Type(name = "TIME_LIMITED_INCREMENTAL_WAIT", value = TimeLimitedIncrementalWaitRetryConfig.class)
})
@Data
@EqualsAndHashCode
@ToString
public abstract class RetryConfig {

    public static final int DEFAULT_ASYNC_MAX_DELAY_LEVELS = 25;

    private final RetryType type;

    private Set<String> retriableExceptions;

    @NotNull
    private RetryMode retryMode = RetryMode.SYNC;

    /**
     * How long an ASYNC retry handoff waits for the broker to confirm the retry copy before requeueing the delivery.
     */
    @NotNull
    @MinDuration(value = 1, unit = TimeUnit.MILLISECONDS)
    private Duration asyncConfirmTimeout = Duration.seconds(5);

    @Min(1)
    private int asyncMaxDelayLevels = DEFAULT_ASYNC_MAX_DELAY_LEVELS;

    @NotNull
    private RetryQueueScope asyncRetryQueueScope = RetryQueueScope.EXCHANGE;

    protected RetryConfig(RetryType type) {
        this.type = type;
    }

    protected RetryConfig(RetryType type,
                          Set<String> retriableExceptions) {
        this.type = type;
        this.retriableExceptions = retriableExceptions;
    }
}
