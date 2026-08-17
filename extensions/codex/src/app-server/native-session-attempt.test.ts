import type { AgentHarnessAttemptParamsV2 } from "openclaw/plugin-sdk/agent-harness-runtime";
import { describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  closeClient: vi.fn(),
  createLifecycle: vi.fn(),
  createServerRequests: vi.fn(),
  createTurnState: vi.fn(),
  createUserInputBridge: vi.fn(),
  prepareConnection: vi.fn(),
  prepareContext: vi.fn(),
  preparePrompt: vi.fn(),
  prepareResources: vi.fn(),
  prepareRuntime: vi.fn(),
  prepareTools: vi.fn(),
  startRuntime: vi.fn(),
  unsubscribe: vi.fn(),
}));

vi.mock("./attempt-client-cleanup.js", () => ({
  closeCodexStartupClientBestEffort: mocks.closeClient,
  CODEX_APP_SERVER_UNSUBSCRIBE_TIMEOUT_MS: 1_000,
  unsubscribeCodexThreadBestEffort: mocks.unsubscribe,
}));
vi.mock("./run-attempt-connection.js", () => ({
  prepareCodexAttemptConnection: mocks.prepareConnection,
}));
vi.mock("./run-attempt-context.js", () => ({ prepareCodexAttemptContext: mocks.prepareContext }));
vi.mock("./run-attempt-lifecycle-controller.js", () => ({
  createCodexAttemptLifecycleController: mocks.createLifecycle,
}));
vi.mock("./run-attempt-prompt.js", () => ({ prepareCodexAttemptPrompt: mocks.preparePrompt }));
vi.mock("./run-attempt-resources.js", () => ({
  prepareCodexAttemptResources: mocks.prepareResources,
}));
vi.mock("./run-attempt-runtime.js", () => ({ prepareCodexAttemptRuntime: mocks.prepareRuntime }));
vi.mock("./run-attempt-server-requests.js", () => ({
  createCodexAttemptServerRequestController: mocks.createServerRequests,
}));
vi.mock("./run-attempt-start.js", () => ({ startCodexAttemptRuntime: mocks.startRuntime }));
vi.mock("./run-attempt-tool-setup.js", () => ({ prepareCodexAttemptTools: mocks.prepareTools }));
vi.mock("./run-attempt-turn-state.js", () => ({
  createCodexAttemptTurnState: mocks.createTurnState,
}));
vi.mock("./user-input-bridge.js", () => ({
  createCodexUserInputBridge: mocks.createUserInputBridge,
}));

import { runCodexAppServerNativeSession } from "./native-session-attempt.js";

function createFixture(run: (session: unknown) => Promise<void>) {
  const upstreamAbort = new AbortController();
  const runAbortController = new AbortController();
  const routeAbortController = new AbortController();
  const client = { request: vi.fn(async () => ({ ok: true })) };
  let routeHandlers:
    | {
        onNotification(notification: { method: string; params?: unknown }): Promise<void>;
        onRequest(request: unknown, scope: { turnId?: string }, signal: AbortSignal): unknown;
      }
    | undefined;
  const route = {
    signal: routeAbortController.signal,
    activate: vi.fn(async (handlers) => {
      routeHandlers = handlers;
    }),
  };
  const resources = {
    state: {
      client,
      thread: { threadId: "thread-1" },
      turnRoute: route,
      nativeHookRelay: undefined,
    },
    registerNativeSubagentMonitor: vi.fn(),
    releaseCurrentRoute: vi.fn(),
    releaseSandboxExecEnvironment: vi.fn(),
    releaseSharedClientLeaseOnce: vi.fn(),
    runCleanupStep: vi.fn(async (_step: string, operation: () => unknown) => operation()),
  };
  const turnState = {
    turnIdRef: { current: undefined as string | undefined },
    turnWatches: { clearAllTimers: vi.fn() },
    userInputBridgeRef: { current: undefined as { cancelPending(): void } | undefined },
  };
  const handleServerRequest = vi.fn(async () => ({ handled: true }));
  const params = {
    sessionId: "session-1",
    abortSignal: upstreamAbort.signal,
    nativeRealtimeSession: { run },
  } as unknown as AgentHarnessAttemptParamsV2;

  mocks.prepareConnection.mockResolvedValueOnce({
    abortFromUpstream: vi.fn(),
    runAbortController,
  });
  mocks.prepareRuntime.mockResolvedValueOnce({});
  mocks.prepareTools.mockResolvedValueOnce({
    scheduledConfiguredMcp: { dispose: vi.fn() },
    scopedMcpTools: { dispose: vi.fn() },
  });
  mocks.prepareContext.mockResolvedValueOnce({});
  mocks.preparePrompt.mockResolvedValueOnce({});
  mocks.prepareResources.mockReturnValueOnce(resources);
  mocks.startRuntime.mockResolvedValueOnce(undefined);
  mocks.createTurnState.mockReturnValueOnce(turnState);
  mocks.createLifecycle.mockReturnValueOnce({});
  mocks.createServerRequests.mockReturnValueOnce({ handleServerRequest });
  mocks.unsubscribe.mockResolvedValueOnce(true);

  return {
    client,
    handleServerRequest,
    params,
    resources,
    route,
    runAbortController,
    routeHandlers: () => routeHandlers,
    turnState,
  };
}

describe("Codex app-server native session", () => {
  it("opens the prepared thread, routes notifications and tools, then cleans up", async () => {
    const onNotification = vi.fn();
    let fixture: ReturnType<typeof createFixture>;
    fixture = createFixture(async (sessionValue) => {
      const session = sessionValue as {
        runtime: string;
        threadId: string;
        request(method: string, params: unknown): Promise<unknown>;
        onNotification(listener: typeof onNotification): () => void;
      };
      expect(session.runtime).toBe("codex-app-server");
      expect(session.threadId).toBe("thread-1");
      session.onNotification(onNotification);
      await session.request("thread/realtime/start", { threadId: "thread-1" });
      await fixture.routeHandlers()?.onNotification({ method: "thread/realtime/started" });
      await fixture
        .routeHandlers()
        ?.onRequest(
          { id: 1, method: "item/tool/call" },
          { turnId: "turn-1" },
          new AbortController().signal,
        );
    });
    const userInputBridge = { cancelPending: vi.fn() };
    mocks.createUserInputBridge.mockReturnValueOnce(userInputBridge);

    await expect(
      runCodexAppServerNativeSession(fixture.params, {} as never),
    ).resolves.toMatchObject({ terminal: { kind: "ok" }, assistantTexts: [] });

    expect(fixture.route.activate).toHaveBeenCalledOnce();
    expect(fixture.client.request).toHaveBeenCalledWith(
      "thread/realtime/start",
      { threadId: "thread-1" },
      undefined,
    );
    expect(onNotification).toHaveBeenCalledWith({ method: "thread/realtime/started" });
    expect(fixture.turnState.turnIdRef.current).toBe("turn-1");
    expect(fixture.handleServerRequest).toHaveBeenCalledOnce();
    expect(fixture.resources.releaseCurrentRoute).toHaveBeenCalledOnce();
    expect(mocks.unsubscribe).toHaveBeenCalledWith(fixture.client, {
      threadId: "thread-1",
      timeoutMs: 1_000,
    });
  });

  it("returns a non-replayable terminal failure from the plugin operation", async () => {
    const fixture = createFixture(async () => {
      throw new Error("native side channel failed");
    });

    await expect(
      runCodexAppServerNativeSession(fixture.params, {} as never),
    ).resolves.toMatchObject({
      terminal: {
        kind: "failed",
        source: "prompt",
        error: expect.objectContaining({ message: "native side channel failed" }),
      },
      replayMetadata: { hadPotentialSideEffects: true, replaySafe: false },
    });
  });
});
