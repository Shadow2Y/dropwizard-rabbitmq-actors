package io.appform.dropwizard.actors.retry.config;

import io.appform.dropwizard.actors.retry.async.TtlRequeueRetryScheduler.IsolationScope;
import io.dropwizard.util.Duration;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TtlRequeueConfig {

    @Valid
    @Min(1)
    private Integer maxRetryQueues;

    @Valid
    private Duration confirmTimeout;

    @Valid
    private IsolationScope queueScope;

    @Valid
    @Min(1)
    private Integer maxPlannedAttempts;

}
