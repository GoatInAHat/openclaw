import type {
  AgentHarnessAttemptParamsV2,
  AgentHarnessAttemptResult,
} from "openclaw/plugin-sdk/agent-harness-runtime";
import {
  closeCodexStartupClientBestEffort,
  CODEX_APP_SERVER_UNSUBSCRIBE_TIMEOUT_MS,
  unsubscribeCodexThreadBestEffort,
} from "./attempt-client-cleanup.js";
import { attemptTerminal } from "./attempt-terminal.js";
import { prepareCodexAttemptConnection } from "./run-attempt-connection.js";
import { prepareCodexAttemptContext } from "./run-attempt-context.js";
import { createCodexAttemptLifecycleController } from "./run-attempt-lifecycle-controller.js";
import { prepareCodexAttemptPrompt } from "./run-attempt-prompt.js";
import { prepareCodexAttemptResources } from "./run-attempt-resources.js";
import { prepareCodexAttemptRuntime } from "./run-attempt-runtime.js";
import { createCodexAttemptServerRequestController } from "./run-attempt-server-requests.js";
import { startCodexAttemptRuntime } from "./run-attempt-start.js";
import { prepareCodexAttemptTools } from "./run-attempt-tool-setup.js";
import { createCodexAttemptTurnState } from "./run-attempt-turn-state.js";
import type { CodexRunAttemptOptions } from "./run-attempt-types.js";
import { createCodexUserInputBridge } from "./user-input-bridge.js";

type NativeSession = Parameters<
  NonNullable<AgentHarnessAttemptParamsV2["nativeRealtimeSession"]>["run"]
>[0];

function buildNativeSessionResult(params: {
  attempt: AgentHarnessAttemptParamsV2;
  failure?: Error;
}): AgentHarnessAttemptResult {
  return {
    terminal: attemptTerminal.normalize({
      promptError: params.failure,
      promptErrorSource: params.failure ? "prompt" : null,
    }),
    sessionIdUsed: params.attempt.sessionId,
    messagesSnapshot: [],
    assistantTexts: [],
    toolMetas: [],
    lastAssistant: undefined,
    currentAttemptAssistant: undefined,
    didSendViaMessagingTool: false,
    messagingToolSentTexts: [],
    messagingToolSentMediaUrls: [],
    messagingToolSentTargets: [],
    messagingToolSourceReplyPayloads: [],
    cloudCodeAssistFormatError: false,
    replayMetadata: { hadPotentialSideEffects: true, replaySafe: false },
    itemLifecycle: { startedCount: 0, completedCount: 0, activeCount: 0 },
  } as AgentHarnessAttemptResult;
}

export async function runCodexAppServerNativeSession(
  params: AgentHarnessAttemptParamsV2,
  options: CodexRunAttemptOptions,
): Promise<AgentHarnessAttemptResult> {
  const operation = params.nativeRealtimeSession;
  if (!operation) {
    throw new Error("Codex native realtime session is missing");
  }

  let connection: Awaited<ReturnType<typeof prepareCodexAttemptConnection>> | undefined;
  let attemptTools: Awaited<ReturnType<typeof prepareCodexAttemptTools>> | undefined;
  let resources: ReturnType<typeof prepareCodexAttemptResources> | undefined;
  let turnRuntime: ReturnType<typeof createCodexAttemptTurnState> | undefined;
  let detachRouteAbort: () => void = () => {};
  const notificationListeners = new Set<
    (notification: { method: string; params?: unknown }) => void | Promise<void>
  >();
  try {
    connection = await prepareCodexAttemptConnection({ params, options });
    const runtime = await prepareCodexAttemptRuntime(connection);
    attemptTools = await prepareCodexAttemptTools(runtime);
    const context = await prepareCodexAttemptContext(runtime, attemptTools);
    const prompt = await prepareCodexAttemptPrompt(context);
    resources = prepareCodexAttemptResources(prompt);
    await startCodexAttemptRuntime(resources);

    turnRuntime = createCodexAttemptTurnState(resources);
    const lifecycle = createCodexAttemptLifecycleController(resources, turnRuntime);
    const serverRequests = createCodexAttemptServerRequestController(
      resources,
      turnRuntime,
      lifecycle,
      { trackTurnActivity: false },
    );
    const { state } = resources;
    const route = state.turnRoute;
    if (!route) {
      throw new Error("Codex native session thread route was not reserved");
    }
    const onRouteAbort = () => connection?.runAbortController.abort(route.signal.reason);
    route.signal.addEventListener("abort", onRouteAbort, { once: true });
    detachRouteAbort = () => route.signal.removeEventListener("abort", onRouteAbort);
    let activeRequestTurnId: string | undefined;
    await route.activate({
      onNotification: async (notification) => {
        for (const listener of notificationListeners) {
          await listener(notification);
        }
      },
      onRequest: (request, scope, signal) => {
        if (scope.turnId && scope.turnId !== activeRequestTurnId) {
          turnRuntime?.userInputBridgeRef.current?.cancelPending();
          activeRequestTurnId = scope.turnId;
          turnRuntime!.turnIdRef.current = scope.turnId;
          turnRuntime!.userInputBridgeRef.current = createCodexUserInputBridge({
            paramsForRun: params,
            threadId: state.thread.threadId,
            turnId: scope.turnId,
            signal: connection!.runAbortController.signal,
          });
        }
        return serverRequests.handleServerRequest(request, scope, signal);
      },
    });
    state.routeActivated = true;
    resources.registerNativeSubagentMonitor(state.thread.threadId);
    const session: NativeSession = {
      runtime: "codex-app-server",
      threadId: state.thread.threadId,
      signal: AbortSignal.any([connection.runAbortController.signal, route.signal]),
      request: <T = unknown>(
        method: string,
        requestParams?: unknown,
        requestOptions?: {
          timeoutMs?: number;
          signal?: AbortSignal;
        },
      ) => state.client.request<T>(method, requestParams, requestOptions),
      onNotification: (listener) => {
        notificationListeners.add(listener);
        return () => notificationListeners.delete(listener);
      },
    };
    try {
      await operation.run(session);
      return buildNativeSessionResult({ attempt: params });
    } catch (error) {
      if (params.abortSignal?.aborted) {
        return buildNativeSessionResult({ attempt: params });
      }
      return buildNativeSessionResult({
        attempt: params,
        failure: error instanceof Error ? error : new Error(String(error)),
      });
    }
  } finally {
    detachRouteAbort();
    notificationListeners.clear();
    if (connection) {
      params.abortSignal?.removeEventListener("abort", connection.abortFromUpstream);
    }
    if (resources) {
      const { state } = resources;
      await resources.runCleanupStep("codex-native-session-user-input", () =>
        turnRuntime?.userInputBridgeRef.current?.cancelPending(),
      );
      await resources.runCleanupStep("codex-native-session-turn-watches", () =>
        turnRuntime?.turnWatches.clearAllTimers(),
      );
      await resources.runCleanupStep("codex-native-session-route", resources.releaseCurrentRoute);
      const nativeHookRelay = state.nativeHookRelay;
      state.nativeHookRelay = undefined;
      await resources.runCleanupStep("codex-native-session-hook-relay", () =>
        nativeHookRelay?.unregister(),
      );
      await resources.runCleanupStep("codex-native-session-unsubscribe", async () => {
        const released = await unsubscribeCodexThreadBestEffort(state.client, {
          threadId: state.thread.threadId,
          timeoutMs: CODEX_APP_SERVER_UNSUBSCRIBE_TIMEOUT_MS,
        });
        if (!released) {
          await closeCodexStartupClientBestEffort(state.client);
        }
      });
      await resources.runCleanupStep("codex-native-session-scoped-mcp", () =>
        attemptTools?.scopedMcpTools?.dispose(),
      );
      await resources.runCleanupStep("codex-native-session-scheduled-mcp", () =>
        attemptTools?.scheduledConfiguredMcp?.dispose(),
      );
      await resources.runCleanupStep(
        "codex-native-session-sandbox",
        resources.releaseSandboxExecEnvironment,
      );
      await resources.runCleanupStep(
        "codex-native-session-shared-client",
        resources.releaseSharedClientLeaseOnce,
      );
    } else {
      try {
        await attemptTools?.scopedMcpTools?.dispose();
      } finally {
        await attemptTools?.scheduledConfiguredMcp?.dispose();
      }
    }
  }
}
