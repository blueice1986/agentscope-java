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
package io.agentscope.harness.agent.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.agent.test.MockModel;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.state.InMemoryAgentStateStore;
import io.agentscope.harness.agent.DistributedStore;
import io.agentscope.harness.agent.HarnessAgent;
import io.agentscope.harness.agent.filesystem.remote.store.InMemoryStore;
import io.agentscope.harness.agent.gateway.channel.ChannelRuntimeContextRequest;
import io.agentscope.harness.agent.gateway.channel.InboundMessage;
import io.agentscope.harness.agent.gateway.channel.OutboundAddress;
import io.agentscope.harness.agent.gateway.channel.chatui.ChatUiChannel;
import io.agentscope.harness.agent.gateway.channel.chatui.SendOptions;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Verifies Gateway merge of caller {@link RuntimeContext} for Channel turns. */
@DisplayName("HarnessGateway RuntimeContext merge")
class HarnessGatewayRuntimeContextMergeTest {

    @Test
    @DisplayName("caller attributes survive; gateway identity fields win")
    void mergeKeepsCallerAttrsAndOverlaysIdentity() {
        HarnessGateway gw = HarnessGateway.create();
        MsgContext ctx = new MsgContext("chatui", null, "sess-a", null, null, Map.of(), "user-1");
        OutboundAddress outbound = OutboundAddress.direct("chatui", "chatui:sess-a");
        RuntimeContext caller =
                RuntimeContext.builder()
                        .sessionId("caller-should-lose")
                        .userId("caller-user")
                        .put("tenant", "acme")
                        .put("outboundAddress", "caller-outbound")
                        .build();

        RuntimeContext merged =
                gw.buildRuntimeContext(ctx, outbound, caller, null, "gw-abc123", "gate-key");

        assertEquals("gw-abc123", merged.getSessionId());
        assertEquals("user-1", merged.getUserId());
        assertEquals("acme", merged.get("tenant"));
        assertSame(outbound, merged.get("outboundAddress"));
        assertEquals("gate-key", merged.get("gateKey"));
        assertSame(ctx, merged.get("msgContext"));
    }

    @Test
    @DisplayName("resolver non-null result replaces caller base")
    void resolverReplacesCallerBase() {
        HarnessGateway gw = HarnessGateway.create();
        AtomicReference<ChannelRuntimeContextRequest> seen = new AtomicReference<>();
        gw.setRuntimeContextResolver(
                req -> {
                    seen.set(req);
                    return RuntimeContext.builder()
                            .put("fromResolver", true)
                            .put("tenant", "resolved")
                            .build();
                });

        MsgContext ctx = new MsgContext("chatui", null, "r", null, null, Map.of(), "u");
        RuntimeContext caller = RuntimeContext.builder().put("tenant", "caller").build();
        InboundMessage inbound = InboundMessage.dm("chatui", "u", List.of(userMsg("hi")));

        RuntimeContext merged = gw.buildRuntimeContext(ctx, null, caller, inbound, "gw-x", "gate");

        assertNotNull(seen.get());
        assertSame(inbound, seen.get().inboundMessage());
        assertSame(caller, seen.get().callerContext());
        assertEquals(true, merged.get("fromResolver"));
        assertEquals("resolved", merged.get("tenant"));
    }

    @Test
    @DisplayName("resolver null keeps caller base")
    void resolverNullKeepsCaller() {
        HarnessGateway gw = HarnessGateway.create();
        gw.setRuntimeContextResolver(req -> null);

        MsgContext ctx = new MsgContext("chatui", null, "r", null, null, Map.of(), "u");
        RuntimeContext caller = RuntimeContext.builder().put("tenant", "caller").build();

        RuntimeContext merged = gw.buildRuntimeContext(ctx, null, caller, null, "gw-x", "gate");

        assertEquals("caller", merged.get("tenant"));
    }

    @Test
    @DisplayName("SendOptions attributes reach the agent via ChatUiChannel")
    void sendOptionsAttributesReachAgent() {
        AtomicReference<RuntimeContext> seen = new AtomicReference<>();
        HarnessAgent agent =
                spy(
                        HarnessAgent.builder()
                                .name("assistant")
                                .sysPrompt("hi")
                                .model(new MockModel("ok"))
                                .build());
        doAnswer(
                        inv -> {
                            seen.set(inv.getArgument(1));
                            return Mono.just(
                                    Msg.builder()
                                            .role(MsgRole.ASSISTANT)
                                            .textContent("ok")
                                            .build());
                        })
                .when(agent)
                .call(anyList(), any(RuntimeContext.class));

        HarnessGateway gateway = HarnessGateway.create();
        gateway.bindMainAgent(agent);
        ChatUiChannel chat = ChatUiChannel.create(gateway);

        SendOptions options =
                SendOptions.userId("user-1")
                        .withAttribute("tenant", "acme")
                        .withAttribute(Foo.class, new Foo("bar"));

        Msg reply = chat.send(options, "hello").block();
        assertNotNull(reply);
        assertNotNull(seen.get());
        assertEquals("acme", seen.get().get("tenant"));
        assertEquals("bar", seen.get().get(Foo.class).value());
        assertEquals("user-1", seen.get().getUserId());
        assertNotNull(seen.get().getSessionId());
        assertTrue(seen.get().getSessionId().startsWith("gw-"));
    }

    @Test
    @DisplayName("InboundMessage.runtimeContext is forwarded on dispatch")
    void inboundRuntimeContextForwarded() {
        AtomicReference<RuntimeContext> seen = new AtomicReference<>();
        HarnessAgent agent =
                spy(
                        HarnessAgent.builder()
                                .name("assistant")
                                .sysPrompt("hi")
                                .model(new MockModel("ok"))
                                .build());
        doAnswer(
                        inv -> {
                            seen.set(inv.getArgument(1));
                            return Mono.just(
                                    Msg.builder()
                                            .role(MsgRole.ASSISTANT)
                                            .textContent("ok")
                                            .build());
                        })
                .when(agent)
                .call(anyList(), any(RuntimeContext.class));

        HarnessGateway gateway = HarnessGateway.create();
        gateway.bindMainAgent(agent);
        ChatUiChannel chat = ChatUiChannel.create(gateway);

        RuntimeContext rtc = RuntimeContext.builder().put("fromInbound", true).build();
        InboundMessage inbound =
                InboundMessage.dm("chatui", "peer-1", List.of(userMsg("hi")))
                        .withRuntimeContext(rtc);

        chat.dispatch(inbound).block();

        assertNotNull(seen.get());
        assertEquals(true, seen.get().get("fromInbound"));
    }

    @Test
    @DisplayName("SendOptions withRuntimeContext replaces prior attributes")
    void withRuntimeContextReplaces() {
        SendOptions options =
                SendOptions.userId("u")
                        .withAttribute("a", 1)
                        .withRuntimeContext(RuntimeContext.builder().put("b", 2).build());
        assertNull(options.runtimeContext().get("a"));
        assertEquals(2, options.runtimeContext().get("b", Integer.class));
    }

    // ------------------------------------------------------------------
    //  Exposed-subagent identity propagation (runSubagent / runSubagentStream)
    // ------------------------------------------------------------------

    @TempDir Path workspace;

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final List<HarnessAgent> agents = new ArrayList<>();

    @AfterEach
    void closeCreatedAgents() {
        agents.forEach(HarnessAgent::close);
    }

    @Test
    @DisplayName("runSubagent carries the exposure userId into the subagent RuntimeContext")
    void runSubagentCarriesExposedUserIdIntoRuntimeContext() {
        AtomicReference<RuntimeContext> seen = new AtomicReference<>();
        HarnessAgent subagent = spySubagent(seen);

        HarnessGateway gateway = HarnessGateway.create();
        String subagentId = gateway.exposeSubagent("coder", "s1", subagent, null, "alice");

        Msg reply = gateway.runSubagent(subagentId, List.of(userMsg("hi"))).block();

        assertNotNull(reply);
        RuntimeContext ctx = seen.get();
        assertNotNull(ctx);
        assertEquals("alice", ctx.getUserId(), "follow-up turn must run as the owning user");
        assertEquals("s1", ctx.getSessionId());
    }

    @Test
    @DisplayName("runSubagentStream carries the exposure userId into the subagent RuntimeContext")
    void runSubagentStreamCarriesExposedUserIdIntoRuntimeContext() {
        AtomicReference<RuntimeContext> seen = new AtomicReference<>();
        HarnessAgent subagent = spySubagent(seen);

        HarnessGateway gateway = HarnessGateway.create();
        String subagentId = gateway.exposeSubagent("coder", "s1", subagent, null, "alice");

        gateway.runSubagentStream(subagentId, List.of(userMsg("hi"))).blockLast();

        RuntimeContext ctx = seen.get();
        assertNotNull(ctx);
        assertEquals("alice", ctx.getUserId(), "follow-up turn must run as the owning user");
        assertEquals("s1", ctx.getSessionId());
    }

    @Test
    @DisplayName("cold-materialized subagent carries the registry userId into its turns")
    void materializedSubagentCarriesRegistryUserIdIntoRuntimeContext() {
        AtomicReference<RuntimeContext> seen = new AtomicReference<>();
        HarnessAgent subagent = spySubagent(seen);

        HarnessGateway gateway = HarnessGateway.create();
        InMemorySubagentRegistry registry = new InMemorySubagentRegistry();
        gateway.setSubagentRegistry(registry);
        AtomicReference<RuntimeContext> materializedWith = new AtomicReference<>();
        gateway.setSubagentMaterializer(
                (agentId, parentRc) -> {
                    materializedWith.set(parentRc);
                    return Optional.of(subagent);
                });
        registry.register(
                new SubagentRecord(
                        "sub-cold", "coder", "s1", "alice", "parent-s1", Instant.now(), null));

        Msg reply = gateway.runSubagent("sub-cold", List.of(userMsg("hi"))).block();

        assertNotNull(reply);
        assertEquals(
                "alice",
                materializedWith.get().getUserId(),
                "materialization context already carries the registry identity");
        RuntimeContext ctx = seen.get();
        assertNotNull(ctx);
        assertEquals("alice", ctx.getUserId(), "the rebuilt session must keep that identity");
        assertEquals("s1", ctx.getSessionId());
    }

    @Test
    @DisplayName("anonymous exposure keeps the legacy anonymous RuntimeContext")
    void anonymousExposedSubagentKeepsAnonymousRuntimeContext() {
        AtomicReference<RuntimeContext> seen = new AtomicReference<>();
        HarnessAgent subagent = spySubagent(seen);

        HarnessGateway gateway = HarnessGateway.create();
        String subagentId = gateway.exposeSubagent("coder", "s1", subagent, null, null);

        gateway.runSubagent(subagentId, List.of(userMsg("hi"))).block();

        RuntimeContext ctx = seen.get();
        assertNotNull(ctx);
        assertNull(ctx.getUserId());
        assertEquals("s1", ctx.getSessionId());
    }

    @Test
    @DisplayName("a real follow-up turn persists state under (alice, s1), never an anonymous slot")
    void runSubagentPersistsTurnStateInOwningUserSlot() {
        InMemoryAgentStateStore stateStore = new InMemoryAgentStateStore();
        DistributedStore distributedStore =
                DistributedStore.builder()
                        .baseStore(new InMemoryStore())
                        .agentStateStore(stateStore)
                        .build();
        HarnessAgent subagent =
                HarnessAgent.builder()
                        .name("coder")
                        .sysPrompt("be brief")
                        .model(new MockModel("done"))
                        .workspace(workspace.resolve("coder"))
                        .distributedStore(distributedStore)
                        .build();
        agents.add(subagent);

        HarnessGateway gateway = HarnessGateway.create();
        String subagentId = gateway.exposeSubagent("coder", "s1", subagent, null, "alice");

        Msg reply = gateway.runSubagent(subagentId, List.of(userMsg("hi"))).block(TIMEOUT);

        assertNotNull(reply);
        boolean aliceSlot = stateStore.exists("alice", "s1");
        boolean anonymousSlot = stateStore.exists(null, "s1");
        assertTrue(
                aliceSlot,
                "turn state must be keyed to the owning user's (alice, s1) slot; anonymous slot"
                        + " present="
                        + anonymousSlot);
        assertFalse(
                anonymousSlot, "no orphan anonymous slot may be created for a user-owned subagent");
    }

    /** Builds a HarnessAgent spy that records the {@link RuntimeContext} of each real turn. */
    private HarnessAgent spySubagent(AtomicReference<RuntimeContext> seen) {
        HarnessAgent agent =
                spy(
                        HarnessAgent.builder()
                                .name("coder")
                                .sysPrompt("hi")
                                .model(new MockModel("ok"))
                                .build());
        agents.add(agent);
        doAnswer(
                        inv -> {
                            seen.set(inv.getArgument(1));
                            return Mono.just(
                                    Msg.builder()
                                            .role(MsgRole.ASSISTANT)
                                            .textContent("ok")
                                            .build());
                        })
                .when(agent)
                .call(anyList(), any(RuntimeContext.class));
        doAnswer(
                        inv -> {
                            seen.set(inv.getArgument(1));
                            return Flux.empty();
                        })
                .when(agent)
                .streamEvents(anyList(), any(RuntimeContext.class));
        return agent;
    }

    private static Msg userMsg(String text) {
        return Msg.builder().role(MsgRole.USER).textContent(text).build();
    }

    private record Foo(String value) {}
}
