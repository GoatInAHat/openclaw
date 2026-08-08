// Resolves persisted session model metadata without loading Gateway projections.
import { normalizeOptionalString } from "@openclaw/normalization-core/string-coerce";
import type { SessionEntry } from "../config/sessions/types.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { DEFAULT_MODEL, DEFAULT_PROVIDER } from "./defaults.js";
import {
  normalizeStoredOverrideModel,
  resolveConfiguredModelRef,
  resolveDefaultModelForAgent,
  resolvePersistedSelectedModelRef,
} from "./model-selection.js";

type SessionModelEntry =
  | SessionEntry
  | Pick<SessionEntry, "model" | "modelProvider" | "modelOverride" | "providerOverride">;

export function resolveSessionModelRef(
  cfg: OpenClawConfig,
  entry?: SessionModelEntry,
  agentId?: string,
  options?: { allowPluginNormalization?: boolean },
): { provider: string; model: string } {
  const normalizedOverride = normalizeStoredOverrideModel({
    providerOverride: entry?.providerOverride,
    modelOverride: entry?.modelOverride,
  });
  if (normalizedOverride.providerOverride && normalizedOverride.modelOverride) {
    return resolvePersistedSelectedModelRef({
      defaultProvider: normalizedOverride.providerOverride,
      overrideProvider: normalizedOverride.providerOverride,
      overrideModel: normalizedOverride.modelOverride,
      allowPluginNormalization: options?.allowPluginNormalization,
    })!;
  }
  const runtimeProvider = normalizeOptionalString(entry?.modelProvider);
  const runtimeModel = normalizeOptionalString(entry?.model);
  if (runtimeProvider && runtimeModel) {
    return { provider: runtimeProvider, model: runtimeModel };
  }

  const resolved = agentId
    ? resolveDefaultModelForAgent({
        cfg,
        agentId,
        allowPluginNormalization: options?.allowPluginNormalization,
      })
    : resolveConfiguredModelRef({
        cfg,
        defaultProvider: DEFAULT_PROVIDER,
        defaultModel: DEFAULT_MODEL,
        allowPluginNormalization: options?.allowPluginNormalization,
      });

  const persisted = resolvePersistedSelectedModelRef({
    defaultProvider: resolved.provider || DEFAULT_PROVIDER,
    runtimeProvider,
    runtimeModel,
    overrideProvider: normalizedOverride.providerOverride,
    overrideModel: normalizedOverride.modelOverride,
    allowPluginNormalization: options?.allowPluginNormalization,
  });
  return persisted ?? resolved;
}
