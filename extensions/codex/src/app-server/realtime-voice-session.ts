import type { AgentHarnessAttemptParams } from "openclaw/plugin-sdk/agent-harness-runtime";
import type {
  RealtimeVoiceBridge,
  RealtimeVoiceBridgeCreateRequest,
  RealtimeVoiceCloseReason,
  RealtimeVoiceRole,
} from "openclaw/plugin-sdk/realtime-voice";
import type { CodexSystemPromptReport } from "./attempt-context.js";
import type { CodexAppServerClient } from "./client.js";
import type { CodexServerNotification, JsonValue } from "./protocol.js";

const CODEX_REALTIME_SAMPLE_RATE_HZ = 24_000;
const CODEX_REALTIME_CHANNELS = 1;
const CODEX_REALTIME_AUDIO_RPC_BYTES = 4_800;
const CODEX_REALTIME_MAX_QUEUED_AUDIO_BYTES =
  CODEX_REALTIME_SAMPLE_RATE_HZ * CODEX_REALTIME_CHANNELS * 2 * 2;
const CODEX_REALTIME_AUDIO_RPC_TIMEOUT_MS = 10_000;

type RealtimeCompletion = {
  promise: Promise<RealtimeVoiceCloseReason>;
  resolve: (reason: RealtimeVoiceCloseReason) => void;
};

function createCompletion(): RealtimeCompletion {
  let resolve!: (reason: RealtimeVoiceCloseReason) => void;
  const promise = new Promise<RealtimeVoiceCloseReason>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

function asRecord(value: JsonValue | undefined): Record<string, JsonValue> | undefined {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, JsonValue>)
    : undefined;
}

function readString(
  record: Record<string, JsonValue> | undefined,
  key: string,
): string | undefined {
  const value = record?.[key];
  return typeof value === "string" ? value : undefined;
}

export class CodexAppServerRealtimeVoiceBridge implements RealtimeVoiceBridge {
  readonly handlesInputAudioBargeIn = true;
  readonly supportsToolResultContinuation = false;
  readonly supportsToolResultSuppression = false;
  readonly completion = createCompletion();
  private connected = false;
  private terminal = false;
  private stopRequested = false;
  private responseTerminalEmitted = false;
  private audioQueue: Buffer[] = [];
  private queuedAudioBytes = 0;
  private audioDrainActive = false;

  constructor(
    private readonly client: CodexAppServerClient,
    private readonly threadId: string,
    private readonly request: RealtimeVoiceBridgeCreateRequest,
    private readonly signal: AbortSignal,
  ) {}

  async connect(): Promise<void> {
    if (this.terminal) {
      throw new Error("Codex realtime voice session is closed");
    }
    if (this.connected) {
      return;
    }
    const providerConfig = this.request.providerConfig;
    const model = readOptionalString(providerConfig.model);
    const voice = readOptionalString(providerConfig.voice);
    const instructions = this.request.instructions?.trim();
    try {
      await this.client.request(
        "thread/realtime/start",
        {
          threadId: this.threadId,
          outputModality: "audio",
          transport: { type: "websocket" },
          version: "v3",
          includeStartupContext: true,
          ...(instructions ? { initialItems: [{ role: "developer", text: instructions }] } : {}),
          ...(model ? { model } : {}),
          ...(voice ? { voice } : {}),
        },
        { signal: this.signal },
      );
      if (this.terminal) {
        throw new Error("Codex realtime voice session closed during startup");
      }
      this.connected = true;
    } catch (error) {
      this.fail(error);
      throw error;
    }
  }

  sendAudio(audio: Buffer): void {
    if (!this.connected || this.terminal || audio.length === 0) {
      return;
    }
    if (this.queuedAudioBytes + audio.length > CODEX_REALTIME_MAX_QUEUED_AUDIO_BYTES) {
      this.fail(new Error("Codex realtime voice input audio queue exceeded two seconds"));
      return;
    }
    this.audioQueue.push(Buffer.from(audio));
    this.queuedAudioBytes += audio.length;
    void this.drainAudioQueue();
  }

  sendUserMessage(text: string): void {
    const normalized = text.trim();
    if (!normalized || !this.connected || this.terminal) {
      return;
    }
    this.responseTerminalEmitted = false;
    void this.client
      .request(
        "thread/realtime/appendText",
        { threadId: this.threadId, role: "user", text: normalized },
        { signal: this.signal },
      )
      .catch((error: unknown) => this.fail(error));
  }

  triggerGreeting(instructions?: string): void {
    const text = instructions?.trim();
    if (text) {
      this.sendUserMessage(text);
    }
  }

  handleBargeIn(): void {
    // Realtime V3 VAD owns cancellation and interruption.
  }

  setMediaTimestamp(): void {}

  submitToolResult(): never {
    throw new Error("Codex realtime voice executes bound agent tools natively");
  }

  acknowledgeMark(): void {}

  close(): void {
    if (this.terminal) {
      return;
    }
    void this.stop().finally(() => this.finish("completed"));
  }

  isConnected(): boolean {
    return this.connected && !this.terminal;
  }

  handleNotification(notification: CodexServerNotification): void {
    const params = asRecord(notification.params);
    if (readString(params, "threadId") !== this.threadId) {
      return;
    }
    switch (notification.method) {
      case "thread/realtime/started":
        this.request.onReady?.();
        return;
      case "thread/realtime/transcript/delta": {
        const role = readRealtimeRole(readString(params, "role"));
        const delta = readString(params, "delta");
        if (role && delta) {
          this.request.onTranscript?.(role, delta, false);
        }
        return;
      }
      case "thread/realtime/transcript/done": {
        const role = readRealtimeRole(readString(params, "role"));
        const text = readString(params, "text");
        if (role) {
          if (text) {
            this.request.onTranscript?.(role, text, true);
          }
          if (role === "user") {
            if (text) {
              this.responseTerminalEmitted = false;
            }
          } else {
            this.emitResponseDone();
          }
        }
        return;
      }
      case "thread/realtime/outputAudio/delta": {
        const audio = asRecord(params?.audio);
        const data = readString(audio, "data");
        if (data) {
          this.request.onAudio(Buffer.from(data, "base64"));
        }
        return;
      }
      case "thread/realtime/itemAdded": {
        const item = asRecord(params?.item);
        const type = readString(item, "type");
        if (type === "input_audio_buffer.speech_started") {
          this.request.onClearAudio("barge-in");
        }
        if (type) {
          this.request.onEvent?.({ direction: "server", type });
          if (type === "response.cancelled") {
            this.responseTerminalEmitted = true;
          }
        }
        return;
      }
      case "thread/realtime/error":
        this.fail(new Error(readString(params, "message") ?? "Codex realtime voice failed"));
        return;
      case "thread/realtime/closed":
        this.finish("completed");
    }
  }

  handleRouteFailure(reason: unknown): void {
    this.fail(reason instanceof Error ? reason : new Error(String(reason)));
  }

  private async stop(): Promise<void> {
    if (this.stopRequested || !this.connected) {
      return;
    }
    this.stopRequested = true;
    await this.client
      .request("thread/realtime/stop", { threadId: this.threadId }, { signal: this.signal })
      .catch(() => undefined);
  }

  private async drainAudioQueue(): Promise<void> {
    if (this.audioDrainActive) {
      return;
    }
    this.audioDrainActive = true;
    try {
      while (this.connected && !this.terminal && this.queuedAudioBytes > 0) {
        const audio = this.takeQueuedAudio(CODEX_REALTIME_AUDIO_RPC_BYTES);
        await this.client.request(
          "thread/realtime/appendAudio",
          {
            threadId: this.threadId,
            audio: {
              data: audio.toString("base64"),
              sampleRate: CODEX_REALTIME_SAMPLE_RATE_HZ,
              numChannels: CODEX_REALTIME_CHANNELS,
              samplesPerChannel: Math.floor(audio.length / 2),
            },
          },
          { signal: this.signal, timeoutMs: CODEX_REALTIME_AUDIO_RPC_TIMEOUT_MS },
        );
      }
    } catch (error) {
      this.fail(error);
    } finally {
      this.audioDrainActive = false;
    }
  }

  private takeQueuedAudio(maxBytes: number): Buffer {
    const parts: Buffer[] = [];
    let remaining = Math.min(maxBytes, this.queuedAudioBytes);
    while (remaining > 0) {
      const head = this.audioQueue[0];
      if (!head) {
        break;
      }
      const length = Math.min(remaining, head.length);
      parts.push(head.subarray(0, length));
      if (length === head.length) {
        this.audioQueue.shift();
      } else {
        this.audioQueue[0] = head.subarray(length);
      }
      this.queuedAudioBytes -= length;
      remaining -= length;
    }
    return Buffer.concat(parts);
  }

  private emitResponseDone(): void {
    if (this.responseTerminalEmitted) {
      return;
    }
    this.responseTerminalEmitted = true;
    this.request.onEvent?.({ direction: "server", type: "response.done" });
  }

  private fail(error: unknown): void {
    if (this.terminal) {
      return;
    }
    this.request.onError?.(error instanceof Error ? error : new Error(String(error)));
    this.finish("error");
  }

  private finish(reason: RealtimeVoiceCloseReason): void {
    if (this.terminal) {
      return;
    }
    this.terminal = true;
    this.connected = false;
    this.audioQueue = [];
    this.queuedAudioBytes = 0;
    this.request.onClose?.(reason);
    this.completion.resolve(reason);
  }
}

export const realtimeVoiceSessionTesting = {
  createBridge: (
    client: CodexAppServerClient,
    threadId: string,
    request: RealtimeVoiceBridgeCreateRequest,
    signal: AbortSignal,
  ) => new CodexAppServerRealtimeVoiceBridge(client, threadId, request, signal),
};

function readOptionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function readRealtimeRole(value: string | undefined): RealtimeVoiceRole | undefined {
  return value === "user" || value === "assistant" ? value : undefined;
}

export function buildRealtimeVoiceAttemptResult(params: {
  attempt: AgentHarnessAttemptParams;
  systemPromptReport: CodexSystemPromptReport;
}) {
  return {
    aborted: false,
    externalAbort: false,
    timedOut: false,
    idleTimedOut: false,
    timedOutDuringCompaction: false,
    promptError: null,
    promptErrorSource: null,
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
    replayMetadata: { hadPotentialSideEffects: false, replaySafe: true },
    itemLifecycle: { startedCount: 0, completedCount: 0, activeCount: 0 },
    systemPromptReport: params.systemPromptReport,
    assistantTranscriptOwned: true,
  };
}

export function assertCodexRealtimeVoiceAudioFormat(
  request: RealtimeVoiceBridgeCreateRequest,
): void {
  const requestedFormat = request.audioFormat;
  if (
    requestedFormat &&
    (requestedFormat.encoding !== "pcm16" ||
      requestedFormat.sampleRateHz !== CODEX_REALTIME_SAMPLE_RATE_HZ ||
      requestedFormat.channels !== CODEX_REALTIME_CHANNELS)
  ) {
    throw new Error("Codex realtime voice requires mono PCM16 audio at 24 kHz");
  }
}
