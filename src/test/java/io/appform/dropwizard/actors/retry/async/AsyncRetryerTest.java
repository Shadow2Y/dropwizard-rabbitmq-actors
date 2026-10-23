package io.appform.dropwizard.actors.retry.async;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.rabbitmq.client.AMQP;
import io.appform.dropwizard.actors.retry.ActorRetryerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AsyncRetryerTest {

    private static final String QUEUE = "queue";
    private static final long STARTED_AT = 1_000L;

    private final AsyncRetryScheduler scheduler = mock(AsyncRetryScheduler.class);
    private final FakeClock clock = new FakeClock(STARTED_AT);
    private final byte[] body = new byte[]{1, 2};

    private static final class FakeClock extends AtomicLong {
        private FakeClock(long now) {
            super(now);
        }
    }

    private AsyncRetryer retryer(ActorRetryerBuilder.Policy<Boolean> policy) {
        return new AsyncRetryer(policy, scheduler, clock::get);
    }

    private void restore(RetryState state) {
        when(scheduler.restoreState(eq(QUEUE), any(), anyLong())).thenReturn(state);
    }

    private void scheduleAndFailOnce(AsyncRetryer retryer, RetryState state) throws Exception {
        restore(state);
        var attempts = new AtomicInteger();
        assertTrue(retryer.call(() -> {
            attempts.incrementAndGet();
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));
        assertEquals(1, attempts.get());
    }

    @Test
    void successfulAndFalseResultsAreTerminal() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        restore(new RetryState(1, STARTED_AT));

        assertTrue(retryer.call(() -> true, QUEUE, body, new AMQP.BasicProperties()));
        assertFalse(retryer.call(() -> false, QUEUE, body, new AMQP.BasicProperties()));
        verify(scheduler, never()).schedule(anyString(), any(), any(), any(), anyLong());
    }

    @Test
    void nonRetriableExceptionIsWrappedAndDoesNotSchedule() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(IllegalStateException.class::isInstance).build());
        var failure = new IOException("not retryable");
        restore(new RetryState(1, STARTED_AT));

        var error = assertThrows(ExecutionException.class,
                () -> retryer.call(() -> {
                    throw failure;
                }, QUEUE, body, new AMQP.BasicProperties()));

        assertSame(failure, error.getCause());
        verify(scheduler, never()).schedule(anyString(), any(), any(), any(), anyLong());
    }

    @Test
    void defaultPolicyDoesNotRetryExceptions() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder().build());
        restore(new RetryState(1, STARTED_AT));

        assertThrows(ExecutionException.class,
                () -> retryer.call(() -> {
                    throw new IOException();
                }, QUEUE, body, new AMQP.BasicProperties()));
        verify(scheduler, never()).schedule(anyString(), any(), any(), any(), anyLong());
    }

    @Test
    void eligibleFailureExecutesOnceAndSchedulesOriginalArgumentsWithNextState() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(IOException.class::isInstance).build());
        var properties = new AMQP.BasicProperties();
        restore(new RetryState(1, STARTED_AT));

        var attempts = new AtomicInteger();
        assertTrue(retryer.call(() -> {
            attempts.incrementAndGet();
            throw new IOException();
        }, QUEUE, body, properties));
        assertEquals(1, attempts.get());

        verify(scheduler).restoreState(QUEUE, properties, STARTED_AT);
        verify(scheduler).schedule(eq(QUEUE), same(body), same(properties),
                eq(new RetryState(2, STARTED_AT)), eq(0L));
        verifyNoMoreInteractions(scheduler);
    }

    @Test
    void schedulerReceivesPolicyDelay() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true)
                .withWaitStrategy(WaitStrategies.fixedWait(100, TimeUnit.MILLISECONDS)).build());

        scheduleAndFailOnce(retryer, new RetryState(1, STARTED_AT));

        verify(scheduler).schedule(eq(QUEUE), same(body), any(), any(), eq(100L));
    }

    @Test
    void schedulingFailureIsNotSwallowedOrReclassified() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        var failure = new IOException("publish failed");
        doThrow(failure).when(scheduler).schedule(eq(QUEUE), same(body), any(), any(), anyLong());
        restore(new RetryState(1, STARTED_AT));

        var error = assertThrows(RetrySchedulingException.class,
                () -> retryer.call(() -> {
                    throw new IllegalStateException();
                }, QUEUE, body, new AMQP.BasicProperties()));

        assertSame(failure, error.getCause());
    }

    @Test
    void permanentSchedulingRejectionIsNotConvertedIntoRequeueFailure() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        var failure = new RetrySchedulingRejectedException("Delay level limit reached");
        doThrow(failure).when(scheduler).schedule(eq(QUEUE), same(body), any(), any(), anyLong());
        restore(new RetryState(1, STARTED_AT));

        var error = assertThrows(RetrySchedulingRejectedException.class,
                () -> retryer.call(() -> {
                    throw new IOException();
                }, QUEUE, body, new AMQP.BasicProperties()));

        assertSame(failure, error);
    }

    @Test
    void transportFailureDelaysNextDeliveryBeforeBusinessExecution() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        restore(new RetryState(1, STARTED_AT));
        doThrow(new IOException("broker unavailable")).when(scheduler)
                .schedule(anyString(), any(), any(), any(), anyLong());
        assertThrows(RetrySchedulingException.class, () -> retryer.call(() -> {
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));

        var attempts = new AtomicInteger();
        var started = System.nanoTime();
        assertTrue(retryer.call(() -> {
            attempts.incrementAndGet();
            return true;
        }, QUEUE, body, new AMQP.BasicProperties()));

        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) >= 900);
        assertEquals(1, attempts.get());
    }

    @Test
    void successfulHandoffClearsCooldownForFollowingDelivery() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        restore(new RetryState(1, STARTED_AT));
        doThrow(new IOException()).doNothing().when(scheduler)
                .schedule(anyString(), any(), any(), any(), anyLong());
        assertThrows(RetrySchedulingException.class, () -> retryer.call(() -> {
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));
        assertTrue(retryer.call(() -> {
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));

        assertTimeoutPreemptively(Duration.ofMillis(500),
                () -> assertTrue(retryer.call(() -> true, QUEUE, body, new AMQP.BasicProperties())));
    }

    @Test
    void shutdownWakesCooldownAndDoesNotExecuteBusinessHandler() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        restore(new RetryState(1, STARTED_AT));
        doThrow(new IOException()).when(scheduler).schedule(anyString(), any(), any(), any(), anyLong());
        assertThrows(RetrySchedulingException.class, () -> retryer.call(() -> {
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));
        var attempts = new AtomicInteger();
        var executor = Executors.newSingleThreadExecutor();
        try {
            var waiting = executor.submit(() -> assertThrows(RetrySchedulingException.class,
                    () -> retryer.call(() -> {
                        attempts.incrementAndGet();
                        return true;
                    }, QUEUE, body, new AMQP.BasicProperties())));
            retryer.prepareToStop();
            waiting.get(500, TimeUnit.MILLISECONDS);
            assertEquals(0, attempts.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void consecutiveTransportFailuresIncreaseCooldown() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        restore(new RetryState(1, STARTED_AT));
        doThrow(new IOException()).when(scheduler).schedule(anyString(), any(), any(), any(), anyLong());
        Callable<Boolean> failing = () -> {
            throw new IOException();
        };
        assertThrows(RetrySchedulingException.class,
                () -> retryer.call(failing, QUEUE, body, new AMQP.BasicProperties()));
        assertThrows(RetrySchedulingException.class,
                () -> retryer.call(failing, QUEUE, body, new AMQP.BasicProperties()));

        var started = System.nanoTime();
        assertTrue(retryer.call(() -> true, QUEUE, body, new AMQP.BasicProperties()));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) >= 1900);
    }

    @Test
    void transportFailureAfterQuietPeriodRestartsCooldownAtInitialBackoff() throws Exception {
        var offset = new AtomicLong();
        var retryer = new AsyncRetryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build(), scheduler, clock::get,
                () -> System.nanoTime() + offset.get());
        restore(new RetryState(1, STARTED_AT));
        doThrow(new IOException("broker unavailable")).when(scheduler)
                .schedule(anyString(), any(), any(), any(), anyLong());
        Callable<Boolean> failing = () -> {
            throw new IOException();
        };
        assertThrows(RetrySchedulingException.class,
                () -> retryer.call(failing, QUEUE, body, new AMQP.BasicProperties()));

        offset.addAndGet(TimeUnit.SECONDS.toNanos(40));
        assertThrows(RetrySchedulingException.class,
                () -> retryer.call(failing, QUEUE, body, new AMQP.BasicProperties()));

        var started = System.nanoTime();
        assertTrue(retryer.call(() -> true, QUEUE, body, new AMQP.BasicProperties()));
        var waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue(waitedMs >= 900 && waitedMs < 1900, "expected the initial 1s cooldown, waited " + waitedMs + "ms");
    }

    @Test
    void interruptionDuringCooldownPreservesInterruptAndSkipsBusinessExecution() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        restore(new RetryState(1, STARTED_AT));
        doThrow(new IOException()).when(scheduler).schedule(anyString(), any(), any(), any(), anyLong());
        assertThrows(RetrySchedulingException.class, () -> retryer.call(() -> {
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));
        var attempts = new AtomicInteger();
        Thread.currentThread().interrupt();
        try {
            var error = assertThrows(RetrySchedulingException.class, () -> retryer.call(() -> {
                attempts.incrementAndGet();
                return true;
            }, QUEUE, body, new AMQP.BasicProperties()));
            assertInstanceOf(InterruptedException.class, error.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(0, attempts.get());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void restartClearsStoppingFlagAndTransportCooldown() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        restore(new RetryState(1, STARTED_AT));
        retryer.start();
        doThrow(new IOException()).when(scheduler).schedule(anyString(), any(), any(), any(), anyLong());
        assertThrows(RetrySchedulingException.class, () -> retryer.call(() -> {
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));
        retryer.stop();
        retryer.start();

        assertTimeoutPreemptively(Duration.ofMillis(500),
                () -> assertTrue(retryer.call(() -> true, QUEUE, body, new AMQP.BasicProperties())));
        retryer.stop();
        verify(scheduler, times(2)).start();
        verify(scheduler, times(2)).stop();
    }

    @Test
    void interruptedSchedulingRestoresInterruptFlag() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        doThrow(new InterruptedException()).when(scheduler)
                .schedule(eq(QUEUE), same(body), any(), any(), anyLong());
        restore(new RetryState(1, STARTED_AT));

        var error = assertThrows(RetrySchedulingException.class,
                () -> retryer.call(() -> {
                    throw new IOException();
                }, QUEUE, body, new AMQP.BasicProperties()));

        assertTrue(error.getCause() instanceof InterruptedException);
        assertTrue(Thread.interrupted());
    }

    @Test
    void countBudgetExhaustionThrowsRetryExceptionWithOriginalAttempt() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true)
                .withStopStrategy(StopStrategies.stopAfterAttempt(3)).build());

        scheduleAndFailOnce(retryer, new RetryState(1, STARTED_AT));
        verify(scheduler, times(1)).schedule(eq(QUEUE), same(body), any(), any(), anyLong());

        scheduleAndFailOnce(retryer, new RetryState(2, STARTED_AT));
        verify(scheduler, times(2)).schedule(eq(QUEUE), same(body), any(), any(), anyLong());

        restore(new RetryState(3, STARTED_AT));
        var error = assertThrows(RetryException.class,
                () -> retryer.call(() -> {
                    throw new IOException();
                }, QUEUE, body, new AMQP.BasicProperties()));

        assertEquals(3, error.getNumberOfFailedAttempts());
        assertEquals(3L, error.getLastFailedAttempt().getAttemptNumber());
        assertTrue(error.getLastFailedAttempt().getExceptionCause() instanceof IOException);
        verify(scheduler, times(2)).schedule(eq(QUEUE), same(body), any(), any(), anyLong());
    }

    @Test
    void clockSkewClampsElapsedTimeWithoutResettingAttemptOrStart() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true)
                .withStopStrategy(attempt -> {
                    assertEquals(0, attempt.getDelaySinceFirstAttempt());
                    assertEquals(2, attempt.getAttemptNumber());
                    return false;
                }).build());
        restore(new RetryState(2, STARTED_AT + 500));

        assertTrue(retryer.call(() -> {
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));

        verify(scheduler).schedule(eq(QUEUE), same(body), any(),
                eq(new RetryState(3, STARTED_AT + 500)), eq(0L));
    }

    @Test
    void timeBudgetUsesWallClockSinceFirstAttemptWithInjectedClock() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true)
                .withStopStrategy(StopStrategies.stopAfterDelay(5_000L)).build());

        scheduleAndFailOnce(retryer, new RetryState(1, STARTED_AT));
        verify(scheduler, times(1)).schedule(eq(QUEUE), same(body), any(), any(), anyLong());

        clock.set(4_000L);
        scheduleAndFailOnce(retryer, new RetryState(2, 500L));
        verify(scheduler, times(2)).schedule(eq(QUEUE), same(body), any(), any(), anyLong());

        clock.set(6_000L);
        restore(new RetryState(3, 500L));
        var error = assertThrows(RetryException.class,
                () -> retryer.call(() -> {
                    throw new IOException();
                }, QUEUE, body, new AMQP.BasicProperties()));

        assertEquals(5_500L, error.getLastFailedAttempt().getDelaySinceFirstAttempt());
        verify(scheduler, times(2)).schedule(eq(QUEUE), same(body), any(), any(), anyLong());
    }

    @Test
    void incrementalDelaysGrowWithRestoredAttemptCount() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true)
                .withWaitStrategy(WaitStrategies.incrementingWait(100, TimeUnit.MILLISECONDS,
                        250, TimeUnit.MILLISECONDS)).build());

        scheduleAndFailOnce(retryer, new RetryState(1, STARTED_AT));
        verify(scheduler).schedule(eq(QUEUE), same(body), any(), eq(new RetryState(2, STARTED_AT)), eq(100L));

        scheduleAndFailOnce(retryer, new RetryState(2, STARTED_AT));
        verify(scheduler).schedule(eq(QUEUE), same(body), any(), eq(new RetryState(3, STARTED_AT)), eq(350L));

        scheduleAndFailOnce(retryer, new RetryState(3, STARTED_AT));
        verify(scheduler).schedule(eq(QUEUE), same(body), any(), eq(new RetryState(4, STARTED_AT)), eq(600L));
    }

    @Test
    void stateIsRestoredBeforeCallableRuns() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true).build());
        var events = new ArrayList<String>();
        when(scheduler.restoreState(eq(QUEUE), any(), anyLong())).thenAnswer(invocation -> {
            events.add("restore");
            return new RetryState(2, STARTED_AT);
        });

        assertTrue(retryer.call(() -> {
            events.add("call");
            throw new IOException();
        }, QUEUE, body, new AMQP.BasicProperties()));

        assertEquals(List.of("restore", "call"), events);
    }

    @Test
    void clockAdvancesElapsedWhileCallableRuns() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true)
                .withStopStrategy(StopStrategies.stopAfterDelay(1_000L)).build());
        restore(new RetryState(4, 2_000L));
        clock.set(2_000L);

        var error = assertThrows(RetryException.class,
                () -> retryer.call(() -> {
                    clock.set(7_500L);
                    throw new IOException();
                }, QUEUE, body, new AMQP.BasicProperties()));

        assertEquals(5_500L, error.getLastFailedAttempt().getDelaySinceFirstAttempt());
        verify(scheduler, never()).schedule(anyString(), any(), any(), any(), anyLong());
    }

    @Test
    void nullSchedulerFailsDescriptivelyBeforeAnyWork() {
        var retryer = ActorRetryerBuilder.<Boolean>newBuilder().build();
        var error = assertThrows(NullPointerException.class,
                () -> new AsyncRetryer(retryer, null, clock::get));

        assertEquals("asyncRetryScheduler", error.getMessage());
    }

    @Test
    void lifecycleDelegatesToScheduler() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder().build());

        retryer.start();
        retryer.stop();

        verify(scheduler).start();
        verify(scheduler).stop();
    }

    @Test
    void startPreparesEveryDelayACountLimitedPolicyCanRequest() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .retryIfException(error -> true)
                .withStopStrategy(StopStrategies.stopAfterAttempt(4))
                .withWaitStrategy(WaitStrategies.incrementingWait(100, TimeUnit.MILLISECONDS,
                        250, TimeUnit.MILLISECONDS)).build());

        retryer.start();

        var order = inOrder(scheduler);
        order.verify(scheduler).start();
        order.verify(scheduler).prepare(Set.of(100L, 350L, 600L));
    }

    @Test
    void timeLimitedPlanningStopsOnceWaitsExhaustTheBudget() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .withStopStrategy(StopStrategies.stopAfterDelay(1_000L))
                .withWaitStrategy(WaitStrategies.incrementingWait(100, TimeUnit.MILLISECONDS,
                        100, TimeUnit.MILLISECONDS)).build());

        assertEquals(Set.of(100L, 200L, 300L, 400L), retryer.plannedDelays());
    }

    @Test
    void singleAttemptPolicyPlansNoDelays() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder()
                .withStopStrategy(StopStrategies.stopAfterAttempt(1)).build());

        assertTrue(retryer.plannedDelays().isEmpty());
    }

    @Test
    void failedPreparationStopsSchedulerAndFailsStart() throws Exception {
        var retryer = retryer(ActorRetryerBuilder.<Boolean>newBuilder().build());
        var failure = new IllegalStateException("too many delay levels");
        doThrow(failure).when(scheduler).prepare(any());

        assertSame(failure, assertThrows(IllegalStateException.class, retryer::start));
        verify(scheduler).stop();
    }
}
