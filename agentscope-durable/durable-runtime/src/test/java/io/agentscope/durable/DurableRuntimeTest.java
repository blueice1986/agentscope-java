/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.agentscope.durable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.durable.DurableTypes.Lifecycle;
import io.agentscope.durable.DurableTypes.SessionKey;
import io.agentscope.durable.DurableTypes.SessionView;
import io.agentscope.durable.DurableTypes.ThreadKey;
import io.agentscope.durable.DurableTypes.ThreadSpec;
import io.agentscope.durable.DurableTypes.ThreadView;
import io.agentscope.durable.DurableTypes.TurnReceipt;
import io.agentscope.durable.DurableTypes.TurnStatus;
import io.agentscope.durable.DurableTypes.TurnView;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

/** Observation semantics of awaited Turns: transient polls must not fail still-running work. */
class DurableRuntimeTest {
    private final SessionKey key = new SessionKey("tenant", "session");
    private final ThreadKey thread = new ThreadKey(key, "main");
    private final ThreadSpec definition =
            new ThreadSpec(thread, new DurableTypes.ThreadBinding("agent", "v1", "1"));

    @Test
    void awaitSurvivesATransientPollFailureAndCompletesWhenTheTurnSucceeds() throws Exception {
        var backend = backend();
        when(backend.turn(thread, "turn-1"))
                .thenReturn(running())
                .thenThrow(
                        new DurableException(
                                DurableException.Code.UNAVAILABLE, "Store unavailable"))
                .thenReturn(running())
                .thenReturn(succeeded());
        try (var runtime = runtime(backend)) {
            var session = runtime.createSession(key, "agent");
            var view =
                    session.call("r1", DurableMessages.Input.text("input"), Duration.ofSeconds(10))
                            .toCompletableFuture()
                            .get(10, TimeUnit.SECONDS);
            assertEquals(TurnStatus.SUCCEEDED, view.status());
        }
    }

    @Test
    void awaitTimesOutInsteadOfFailingWhenPollsKeepFailingUntilTheDeadline() {
        var backend = backend();
        when(backend.turn(thread, "turn-1"))
                .thenThrow(
                        new DurableException(
                                DurableException.Code.UNAVAILABLE, "Store unavailable"));
        try (var runtime = runtime(backend)) {
            var session = runtime.createSession(key, "agent");
            var future =
                    session.submit("r1", "input")
                            .await(Duration.ofMillis(200))
                            .toCompletableFuture();
            var error =
                    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
            assertInstanceOf(TimeoutException.class, error.getCause());
            assertTrue(error.getCause().getMessage().contains("turn-1"));
        }
    }

    /** Minimal Backend whose user operations answer with schema-valid canned views. */
    private SessionBackend backend() {
        var backend = mock(SessionBackend.class);
        when(backend.wakeups()).thenReturn(Wakeups.none());
        var view = new SessionView(key, "main", Lifecycle.OPEN, null);
        when(backend.create(any())).thenReturn(view);
        when(backend.session(key)).thenReturn(view);
        when(backend.thread(thread)).thenReturn(new ThreadView(definition, Lifecycle.OPEN));
        when(backend.submit(eq(thread), eq("r1"), any()))
                .thenReturn(new TurnReceipt(thread, "r1", "turn-1"));
        return backend;
    }

    private DurableRuntime runtime(SessionBackend backend) {
        return DurableRuntime.builder()
                .backend(backend)
                .definition("agent", "v1", "1")
                .observationInterval(Duration.ofMillis(10))
                .build();
    }

    private TurnView running() {
        return new TurnView(
                thread,
                "turn-1",
                TurnStatus.RUNNING,
                1,
                1,
                "msg-1",
                false,
                null,
                null,
                null,
                null,
                null);
    }

    private TurnView succeeded() {
        return new TurnView(
                thread,
                "turn-1",
                TurnStatus.SUCCEEDED,
                2,
                1,
                "msg-1",
                false,
                DurableJson.value(null),
                null,
                null,
                null,
                null);
    }
}
