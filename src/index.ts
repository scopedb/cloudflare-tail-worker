/*
 * Copyright 2026 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Client } from "scopedb";

export interface Env {
  SCOPEDB_ENDPOINT: string;
  SCOPEDB_API_KEY: string;
  SCOPEDB_DATABASE?: string;
  SCOPEDB_SCHEMA?: string;
  SCOPEDB_TABLE?: string;
}

export interface TailRequestEvent {
  readonly request?: {
    readonly url?: string;
    readonly method?: string;
    readonly headers?: Readonly<Record<string, string>>;
    readonly cf?: Readonly<Record<string, unknown>>;
  };
  readonly response?: {
    readonly status?: number;
  };
}

export interface TailLog {
  readonly timestamp?: number | null;
  readonly level: string;
  readonly message: readonly unknown[];
}

export interface TailException {
  readonly timestamp?: number | null;
  readonly name: string;
  readonly message: string;
  readonly stack?: string;
}

export interface TailScriptVersion {
  readonly id?: string;
  readonly tag?: string;
  readonly message?: string;
}

export interface TailItem {
  readonly scriptName?: string | null;
  readonly scriptVersion?: TailScriptVersion | null;
  readonly outcome: string;
  readonly eventTimestamp?: number | null;
  readonly wallTime?: number;
  readonly cpuTime?: number;
  readonly truncated?: boolean;
  readonly event?: TailRequestEvent | null;
  readonly logs?: readonly TailLog[];
  readonly exceptions?: readonly TailException[];
}

export type StructuredTelemetryEventType =
  | "rum.pageview"
  | "rum.vital"
  | "rum.action"
  | "rum.error"
  | "trace.span"
  | `app.${string}`;

export type CloudflareWorkerEventType =
  | "worker.invocation"
  | "worker.log"
  | "worker.exception"
  | StructuredTelemetryEventType;

export interface StructuredTelemetryEvent {
  readonly event_type: StructuredTelemetryEventType;
  readonly occurred_at?: number | string;
  readonly level?: "debug" | "info" | "warn" | "error";
  readonly outcome?: string;
  readonly message?: string;
  readonly method?: string | null;
  readonly host?: string | null;
  readonly path?: string | null;
  readonly status?: number | null;
  readonly trace_id?: string | null;
  readonly span_id?: string | null;
  readonly parent_span_id?: string | null;
  readonly duration_ms?: number | null;
  readonly referrer?: string | null;
  readonly attributes?: Readonly<Record<string, unknown>>;
}

export interface StructuredTelemetryEnvelope {
  readonly __scopedb_telemetry: 1;
  readonly environment?: string;
  readonly app_version?: string;
  readonly events?: readonly StructuredTelemetryEvent[];
  readonly event?: StructuredTelemetryEvent;
}

export interface CloudflareWorkerEventRow extends Record<string, unknown> {
  event_id: string;
  occurred_at: string;
  service: string;
  event_type: CloudflareWorkerEventType;
  outcome: string;
  level: "debug" | "info" | "warn" | "error";
  method: string | null;
  host: string | null;
  path: string | null;
  status: number | null;
  ray_id: string | null;
  message: string;
  attributes: Record<string, unknown>;
}

export interface UrlCaptureOptions {
  /** Capture only these query keys. By default all keys are captured (with sensitive keys/tokens redacted); [] disables query capture. */
  readonly allowedQueryParams?: readonly string[];
  /** Applied to each decoded query value. Return null to omit that value. */
  readonly redactQueryValue?: (key: string, value: string) => string | null;
  /** Applied to request, Referer, and structured telemetry pathnames before token redaction. Return null to omit a path. */
  readonly redactPath?: (pathname: string, hostname: string) => string | null;
}

export interface TailWorkerOptions extends UrlCaptureOptions {
  readonly fetch?: typeof globalThis.fetch;
  readonly shouldRecord?: (item: TailItem) => boolean;
}

const DEFAULT_DATABASE = "scopedb";
const DEFAULT_SCHEMA = "public";
const DEFAULT_TABLE = "cloudflare_worker_events";

const MIN_INT64 = BigInt("-9223372036854775808");
const MAX_INT64 = BigInt("9223372036854775807");
const MAX_SANITIZE_DEPTH = 8;
const MAX_USER_AGENT_LENGTH = 512;
const MAX_ACCEPT_LENGTH = 256;
const MAX_MESSAGE_LENGTH = 2048;
const MAX_STACK_LENGTH = 8192;
const MAX_PATH_LENGTH = 2048;
const MAX_STRUCTURED_LOG_CHARS = 131_072;
const MAX_STRUCTURED_EVENTS_PER_LOG = 64;

const STRUCTURED_EVENT_TYPE_PATTERN =
  /^(?:rum\.(?:pageview|vital|action|error)|trace\.span|app\.[a-z0-9][a-z0-9_.-]{0,63})$/;

const DEFAULT_SENSITIVE_QUERY_KEYS = new Set([
  "code",
  "state",
  "token",
  "access_token",
  "refresh_token",
  "id_token",
  "api_key",
  "apikey",
  "key",
  "secret",
  "client_secret",
  "password",
  "passwd",
  "signature",
  "sig",
  "x-amz-signature",
  "x-amz-security-token",
  "authorization",
  "auth",
  "session",
  "session_id",
  "cookie",
  "otp",
  "invite",
  "invitation",
]);

const ENGLISH_LONG_WORD_AFFIX =
  /(?:tion|sion|ment|ness|able|ible|ally|ical|istic|ence|ance|izer|ising|izing|ities|ology|graph|inter|trans|counter|under|micro|macro|multi|super|hyper|electr|character|institut|revolution|communicat|administrat|organization|authentication|authorization|configuration|documentation|implementation|instrumentation|observability|compatibility|vulnerability|infrastructure|troubleshooting)/i;

const TRACEPARENT_PATTERN = /^00-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$/i;
const TRACE_ID_PATTERN = /^[0-9a-f]{32}$/i;
const SPAN_ID_PATTERN = /^[0-9a-f]{16}$/i;

interface EdgeContext {
  readonly invocationId: string;
  readonly service: string;
  readonly rayId: string | null;
  readonly requestHost: string | null;
  readonly requestTraceId: string | null;
  readonly truncated: boolean;
  readonly scriptVersionId: string | null;
  readonly scriptEnvironment: string | null;
  readonly userAgent: string | null;
  readonly colo: string | null;
  readonly continent: string | null;
  readonly country: string | null;
  readonly region: string | null;
  readonly city: string | null;
  readonly timezone: string | null;
  readonly asn: number | null;
  readonly asOrganization: string | null;
}

interface ParsedStructuredEnvelope {
  readonly environment: string | null;
  readonly appVersion: string | null;
  readonly events: readonly unknown[];
}

export function createTailWorker(options: TailWorkerOptions = {}) {
  return {
    async tail(events: readonly TailItem[], env: Env): Promise<void> {
      const endpoint = requiredBinding(env.SCOPEDB_ENDPOINT, "SCOPEDB_ENDPOINT");
      const apiKey = requiredBinding(env.SCOPEDB_API_KEY, "SCOPEDB_API_KEY");
      const database = nonEmptyString(env.SCOPEDB_DATABASE) ?? DEFAULT_DATABASE;
      const schema = nonEmptyString(env.SCOPEDB_SCHEMA) ?? DEFAULT_SCHEMA;
      const table = nonEmptyString(env.SCOPEDB_TABLE) ?? DEFAULT_TABLE;

      const filtered = options.shouldRecord ? events.filter(options.shouldRecord) : events;
      const rows = tailItemsToRows(filtered, options);
      if (rows.length === 0) {
        return;
      }

      const client = new Client(endpoint, {
        apiKey,
        ...(options.fetch !== undefined ? { fetch: options.fetch } : {}),
      });
      const stream = client
        .table(table, { database, schema })
        .appendStream()
        .strictEncoding(true)
        .maxConcurrentBatches(1)
        .attemptTimeoutMs(10_000)
        .maxElapsedTimeMs(25_000)
        .maxRetries(2)
        .build();

      try {
        await stream.sendAll(rows);
      } finally {
        await stream.shutdown();
      }
    },
  };
}

export default createTailWorker();

export function tailItemsToRows(
  events: readonly TailItem[],
  options: UrlCaptureOptions = {},
): CloudflareWorkerEventRow[] {
  return events.flatMap((item) => tailItemToRows(item, options));
}

export function tailItemToRows(
  item: TailItem,
  options: UrlCaptureOptions = {},
): CloudflareWorkerEventRow[] {
  const invocationId = crypto.randomUUID();
  const occurredAt = formatTimestamp(item.eventTimestamp);
  const service = nonEmptyString(item.scriptName) ?? "unknown";
  const outcome = nonEmptyString(item.outcome) ?? "unknown";
  const request = item.event?.request;
  const method = nonEmptyString(request?.method);
  const { host, path, query } = extractUrlMetadata(request?.url, options);
  const referer = extractUrlMetadata(
    headerValue(request?.headers, "referer") ?? undefined,
    options,
    false,
  );
  const status = safeHttpStatus(item.event?.response?.status);
  const rayId = headerValue(request?.headers, "cf-ray");
  const truncated = Boolean(item.truncated);
  const logs = item.logs ?? [];
  const exceptions = item.exceptions ?? [];
  const requestTraceId = extractTraceId(headerValue(request?.headers, "traceparent"));
  const userAgent = boundedString(
    headerValue(request?.headers, "user-agent"),
    MAX_USER_AGENT_LENGTH,
  );

  const edge: EdgeContext = {
    invocationId,
    service,
    rayId,
    requestHost: host,
    requestTraceId,
    truncated,
    scriptVersionId: stringField(item.scriptVersion?.id),
    scriptEnvironment: nonEmptyString(
      (item as { readonly scriptEnvironment?: unknown }).scriptEnvironment,
    ),
    userAgent,
    colo: stringField(request?.cf?.["colo"]),
    continent: stringField(request?.cf?.["continent"]),
    country: stringField(request?.cf?.["country"]),
    region: stringField(request?.cf?.["region"]),
    city: stringField(request?.cf?.["city"]),
    timezone: stringField(request?.cf?.["timezone"]),
    asn: safeInteger(request?.cf?.["asn"]),
    asOrganization: stringField(request?.cf?.["asOrganization"]),
  };

  const childRows: CloudflareWorkerEventRow[] = [];
  let rawLogCount = 0;

  for (const log of logs) {
    const structured = tryParseStructuredTelemetry(log);
    if (structured !== null) {
      const fallbackEpochMs = log.timestamp ?? item.eventTimestamp;
      for (const entry of structured.events) {
        const structuredRow = structuredEventToRow(
          entry,
          structured,
          fallbackEpochMs,
          edge,
          options,
        );
        if (structuredRow !== null) {
          childRows.push(structuredRow);
        }
      }
      continue;
    }

    rawLogCount += 1;
    const sanitizedArgs = sanitizeValue(log.message, 0, new Set());
    childRows.push({
      event_id: crypto.randomUUID(),
      occurred_at: formatTimestamp(log.timestamp ?? item.eventTimestamp),
      service,
      event_type: "worker.log",
      outcome,
      level: normalizeLogLevel(log.level),
      method,
      host,
      path,
      status,
      ray_id: rayId,
      message: formatLogMessage(sanitizedArgs),
      attributes: {
        invocation_id: invocationId,
        raw_level: log.level,
        truncated,
        arguments: sanitizedArgs,
      },
    });
  }

  for (const exception of exceptions) {
    const exceptionName = nonEmptyString(exception.name) ?? "Error";
    const exceptionMessage = typeof exception.message === "string" ? exception.message : "";
    childRows.push({
      event_id: crypto.randomUUID(),
      occurred_at: formatTimestamp(exception.timestamp ?? item.eventTimestamp),
      service,
      event_type: "worker.exception",
      outcome,
      level: "error",
      method,
      host,
      path,
      status,
      ray_id: rayId,
      message:
        boundedString(
          exceptionMessage.length > 0 ? `${exceptionName}: ${exceptionMessage}` : exceptionName,
          MAX_MESSAGE_LENGTH,
        ) ?? exceptionName,
      attributes: {
        invocation_id: invocationId,
        exception_name: exceptionName,
        stack: boundedString(exception.stack ?? null, MAX_STACK_LENGTH),
        truncated,
      },
    });
  }

  const invocationRow: CloudflareWorkerEventRow = {
    event_id: invocationId,
    occurred_at: occurredAt,
    service,
    event_type: "worker.invocation",
    outcome,
    level: invocationLevel(outcome, status, exceptions.length),
    method,
    host,
    path,
    status,
    ray_id: rayId,
    message: formatInvocationMessage(method, path, status, outcome),
    attributes: {
      invocation_id: invocationId,
      wall_time_ms: finiteNumber(item.wallTime),
      cpu_time_ms: finiteNumber(item.cpuTime),
      log_count: rawLogCount,
      exception_count: exceptions.length,
      truncated,
      user_agent: userAgent,
      accept: boundedString(headerValue(request?.headers, "accept"), MAX_ACCEPT_LENGTH),
      sec_fetch_mode: stringField(headerValue(request?.headers, "sec-fetch-mode")),
      sec_fetch_dest: stringField(headerValue(request?.headers, "sec-fetch-dest")),
      is_prefetch: isPrefetchRequest(request?.headers),
      is_rsc: headerValue(request?.headers, "rsc") === "1",
      trace_id: requestTraceId,
      referer_host: referer.host,
      referer_path: referer.path,
      query,
      colo: edge.colo,
      continent: edge.continent,
      country: edge.country,
      region: edge.region,
      city: edge.city,
      timezone: edge.timezone,
      asn: edge.asn,
      as_organization: edge.asOrganization,
      http_protocol: stringField(request?.cf?.["httpProtocol"]),
      tls_version: stringField(request?.cf?.["tlsVersion"]),
      verified_bot_category: extractVerifiedBotCategory(request?.cf),
    },
  };

  return [invocationRow, ...childRows];
}

function tryParseStructuredTelemetry(log: TailLog): ParsedStructuredEnvelope | null {
  if (!Array.isArray(log.message) || log.message.length !== 1) {
    return null;
  }

  const first = log.message[0];
  let candidate: Record<string, unknown> | null = null;

  if (isPlainObject(first) && first["__scopedb_telemetry"] === 1) {
    candidate = first;
  } else if (
    typeof first === "string" &&
    first.length <= MAX_STRUCTURED_LOG_CHARS &&
    first.includes('"__scopedb_telemetry"')
  ) {
    try {
      const parsed: unknown = JSON.parse(first);
      if (isPlainObject(parsed) && parsed["__scopedb_telemetry"] === 1) {
        candidate = parsed;
      }
    } catch {
      return null;
    }
  }

  if (candidate === null) {
    return null;
  }

  let events: readonly unknown[];
  if (Array.isArray(candidate["events"])) {
    events = candidate["events"].slice(0, MAX_STRUCTURED_EVENTS_PER_LOG);
  } else if (isPlainObject(candidate["event"])) {
    events = [candidate["event"]];
  } else {
    return null;
  }

  return {
    environment: boundedString(nonEmptyString(candidate["environment"]), 64),
    appVersion: boundedString(nonEmptyString(candidate["app_version"]), 128),
    events,
  };
}

function structuredEventToRow(
  rawEntry: unknown,
  envelope: ParsedStructuredEnvelope,
  fallbackEpochMs: number | null | undefined,
  edge: EdgeContext,
  options: UrlCaptureOptions,
): CloudflareWorkerEventRow | null {
  if (!isPlainObject(rawEntry)) {
    return null;
  }

  const rawType = nonEmptyString(rawEntry["event_type"]);
  if (rawType === null || !STRUCTURED_EVENT_TYPE_PATTERN.test(rawType)) {
    return null;
  }
  const eventType = rawType as StructuredTelemetryEventType;

  const rawAttrs = isPlainObject(rawEntry["attributes"]) ? rawEntry["attributes"] : {};
  const sanitizedAttrs = sanitizeStructuredAttributes(rawAttrs);

  const occurredAt = formatFlexibleTimestamp(rawEntry["occurred_at"], fallbackEpochMs);

  const explicitHost = boundedString(nonEmptyString(rawEntry["host"]), 255);
  const baseHost = explicitHost ?? edge.requestHost;

  const routeFallback =
    eventType === "trace.span"
      ? (nonEmptyString(sanitizedAttrs["http_route"]) ??
          nonEmptyString(sanitizedAttrs["http.route"]))
      : null;
  const rawPathInput =
    nonEmptyString(rawEntry["path"]) ??
    (routeFallback !== null && routeFallback.startsWith("/") ? routeFallback : null);

  let eventHost = baseHost;
  let eventPath: string | null = null;
  let extractedQuery: Record<string, string[]> = {};

  if (rawPathInput !== null) {
    if (/^https?:\/\//i.test(rawPathInput)) {
      const parsedUrl = extractUrlMetadata(rawPathInput, options, true);
      if (explicitHost === null && parsedUrl.host !== null) {
        eventHost = parsedUrl.host;
      }
      eventPath = parsedUrl.path;
      extractedQuery = parsedUrl.query;
    } else {
      const normalizedRel = rawPathInput.startsWith("/") ? rawPathInput : `/${rawPathInput}`;
      const syntheticOrigin = `https://${eventHost ?? "placeholder.invalid"}`;
      const parsedUrl = extractUrlMetadata(`${syntheticOrigin}${normalizedRel}`, options, true);
      eventPath = parsedUrl.path;
      extractedQuery = parsedUrl.query;
    }
  }

  const rawReferrer =
    nonEmptyString(rawEntry["referrer"]) ??
    nonEmptyString(rawEntry["referer"]) ??
    nonEmptyString(sanitizedAttrs["referrer"]) ??
    nonEmptyString(sanitizedAttrs["referer"]);
  delete sanitizedAttrs["referrer"];
  delete sanitizedAttrs["referer"];

  let refererHost = boundedString(nonEmptyString(sanitizedAttrs["referer_host"]), 255);
  let refererPath =
    typeof sanitizedAttrs["referer_path"] === "string"
      ? redactPathname(sanitizedAttrs["referer_path"], refererHost ?? "", options)
      : null;

  if (rawReferrer !== null) {
    const parsedRef = /^https?:\/\//i.test(rawReferrer)
      ? extractUrlMetadata(rawReferrer, options, false)
      : rawReferrer.startsWith("/")
        ? extractUrlMetadata(
            `https://${eventHost ?? "placeholder.invalid"}${rawReferrer}`,
            options,
            false,
          )
        : { host: null, path: null };
    if (refererHost === null) {
      refererHost = parsedRef.host;
    }
    if (refererPath === null) {
      refererPath = parsedRef.path;
    }
  }

  const rawMethod =
    nonEmptyString(rawEntry["method"]) ??
    (eventType === "trace.span"
      ? (nonEmptyString(sanitizedAttrs["http_method"]) ??
          nonEmptyString(sanitizedAttrs["http.request.method"]))
      : null);
  const eventMethod = rawMethod !== null ? boundedString(rawMethod.toUpperCase(), 32) : null;

  const statusCandidate =
    typeof rawEntry["status"] === "number"
      ? rawEntry["status"]
      : eventType === "trace.span" && typeof sanitizedAttrs["http.response.status_code"] === "number"
        ? sanitizedAttrs["http.response.status_code"]
        : undefined;
  const eventStatus = safeHttpStatus(statusCandidate);

  const explicitLevel = nonEmptyString(rawEntry["level"]);
  const statusCodeAttr = nonEmptyString(sanitizedAttrs["status_code"])?.toUpperCase() ?? null;
  const explicitOutcome = boundedString(nonEmptyString(rawEntry["outcome"]), 64);

  let eventLevel: "debug" | "info" | "warn" | "error";
  if (explicitLevel !== null) {
    eventLevel = normalizeLogLevel(explicitLevel);
  } else if (
    eventType === "rum.error" ||
    explicitOutcome?.toLowerCase() === "error" ||
    statusCodeAttr === "ERROR" ||
    (eventStatus !== null && eventStatus >= 500)
  ) {
    eventLevel = "error";
  } else if (eventStatus !== null && eventStatus >= 400) {
    eventLevel = "warn";
  } else {
    eventLevel = "info";
  }

  const eventOutcome = explicitOutcome ?? (eventLevel === "error" ? "error" : "ok");

  const traceId =
    normalizeHexId(rawEntry["trace_id"], TRACE_ID_PATTERN) ??
    normalizeHexId(sanitizedAttrs["trace_id"], TRACE_ID_PATTERN) ??
    edge.requestTraceId;
  const spanId =
    normalizeHexId(rawEntry["span_id"], SPAN_ID_PATTERN) ??
    normalizeHexId(sanitizedAttrs["span_id"], SPAN_ID_PATTERN);
  const parentSpanId =
    normalizeHexId(rawEntry["parent_span_id"], SPAN_ID_PATTERN) ??
    normalizeHexId(sanitizedAttrs["parent_span_id"], SPAN_ID_PATTERN);
  const rawDuration =
    finiteNumber(rawEntry["duration_ms"]) ?? finiteNumber(sanitizedAttrs["duration_ms"]);
  const durationMs = rawDuration !== null && rawDuration >= 0 ? rawDuration : null;

  const appVersion = boundedString(
    nonEmptyString(sanitizedAttrs["app_version"]) ?? envelope.appVersion,
    128,
  );
  const environment = boundedString(
    edge.scriptEnvironment ??
      nonEmptyString(sanitizedAttrs["environment"]) ??
      nonEmptyString(sanitizedAttrs["deployment.environment.name"]) ??
      envelope.environment ??
      (edge.service.endsWith("-staging") ? "staging" : null),
    64,
  );

  const message =
    boundedString(nonEmptyString(rawEntry["message"]), MAX_MESSAGE_LENGTH) ??
    formatStructuredMessage(eventType, eventMethod, eventPath, sanitizedAttrs);

  const mergedAttributes: Record<string, unknown> = {
    ...sanitizedAttrs,
    ...(sanitizedAttrs["query"] === undefined && Object.keys(extractedQuery).length > 0
      ? { query: extractedQuery }
      : {}),
    ...(refererHost !== null ? { referer_host: refererHost } : {}),
    ...(refererPath !== null ? { referer_path: refererPath } : {}),
    ...(appVersion !== null ? { app_version: appVersion } : {}),
    environment,
    trace_id: traceId,
    ...(spanId !== null || eventType === "trace.span" ? { span_id: spanId } : {}),
    ...(parentSpanId !== null || eventType === "trace.span"
      ? { parent_span_id: parentSpanId }
      : {}),
    ...(durationMs !== null ? { duration_ms: durationMs } : {}),
    invocation_id: edge.invocationId,
    truncated: edge.truncated,
    script_version_id: edge.scriptVersionId,
    user_agent: edge.userAgent,
    colo: edge.colo,
    continent: edge.continent,
    country: edge.country,
    region: edge.region,
    city: edge.city,
    timezone: edge.timezone,
    asn: edge.asn,
    as_organization: edge.asOrganization,
  };

  return {
    event_id: crypto.randomUUID(),
    occurred_at: occurredAt,
    service: edge.service,
    event_type: eventType,
    outcome: eventOutcome,
    level: eventLevel,
    method: eventMethod,
    host: eventHost,
    path: eventPath,
    status: eventStatus,
    ray_id: edge.rayId,
    message,
    attributes: mergedAttributes,
  };
}

function sanitizeStructuredAttributes(
  input: Readonly<Record<string, unknown>>,
): Record<string, unknown> {
  const sanitized = sanitizeValue(input, 0, new Set());
  if (!isPlainObject(sanitized)) {
    return {};
  }
  return boundObjectStrings(sanitized, 0) as Record<string, unknown>;
}

function boundObjectStrings(value: unknown, depth: number, keyName?: string): unknown {
  if (typeof value === "string") {
    const limit = keyName === "stack" ? MAX_STACK_LENGTH : MAX_MESSAGE_LENGTH;
    return value.length <= limit ? value : value.slice(0, limit);
  }
  if (depth >= MAX_SANITIZE_DEPTH || value === null || typeof value !== "object") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => boundObjectStrings(item, depth + 1));
  }
  const output: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    output[key] = boundObjectStrings(entry, depth + 1, key);
  }
  return output;
}

function formatStructuredMessage(
  eventType: StructuredTelemetryEventType,
  method: string | null,
  path: string | null,
  attrs: Readonly<Record<string, unknown>>,
): string {
  if (eventType === "rum.pageview") {
    return boundedString(`pageview ${path ?? "/"}`, MAX_MESSAGE_LENGTH) ?? "pageview /";
  }
  if (eventType === "rum.vital") {
    const vitalName = nonEmptyString(attrs["vital_name"]) ?? "vital";
    const vitalValue = finiteNumber(attrs["vital_value"]);
    return (
      boundedString(
        vitalValue !== null ? `${vitalName} ${vitalValue}` : vitalName,
        MAX_MESSAGE_LENGTH,
      ) ?? vitalName
    );
  }
  if (eventType === "rum.action") {
    return (
      boundedString(nonEmptyString(attrs["action_name"]) ?? "rum.action", MAX_MESSAGE_LENGTH) ??
      "rum.action"
    );
  }
  if (eventType === "rum.error") {
    return (
      boundedString(nonEmptyString(attrs["error_type"]) ?? "Error", MAX_MESSAGE_LENGTH) ?? "Error"
    );
  }
  if (eventType === "trace.span") {
    const spanName = nonEmptyString(attrs["span_name"]);
    if (spanName !== null) {
      return boundedString(spanName, MAX_MESSAGE_LENGTH) ?? "trace.span";
    }
    if (method !== null && path !== null) {
      return boundedString(`${method} ${path}`, MAX_MESSAGE_LENGTH) ?? "trace.span";
    }
    return "trace.span";
  }
  return eventType;
}

function invocationLevel(
  outcome: string,
  status: number | null,
  exceptionCount: number,
): "info" | "warn" | "error" {
  if (
    exceptionCount > 0 ||
    (status !== null && status >= 500) ||
    (outcome !== "ok" && outcome !== "canceled")
  ) {
    return "error";
  }
  if (status !== null && status >= 400) {
    return "warn";
  }
  return "info";
}

function normalizeLogLevel(level: string | undefined): "debug" | "info" | "warn" | "error" {
  const lower = typeof level === "string" ? level.toLowerCase() : "info";
  if (lower === "error") {
    return "error";
  }
  if (lower === "warn" || lower === "warning") {
    return "warn";
  }
  if (lower === "debug" || lower === "trace") {
    return "debug";
  }
  return "info";
}

function formatInvocationMessage(
  method: string | null,
  path: string | null,
  status: number | null,
  outcome: string,
): string {
  if (method !== null && path !== null) {
    return status !== null ? `${method} ${path} -> ${status}` : `${method} ${path} (${outcome})`;
  }
  if (path !== null) {
    return status !== null ? `${path} -> ${status}` : `${path} (${outcome})`;
  }
  return `invocation (${outcome})`;
}

function formatLogMessage(sanitizedArgs: unknown): string {
  if (!Array.isArray(sanitizedArgs) || sanitizedArgs.length === 0) {
    return "";
  }
  const parts = sanitizedArgs.map((part) => {
    if (typeof part === "string") {
      return part;
    }
    if (
      part === null ||
      typeof part === "number" ||
      typeof part === "boolean" ||
      typeof part === "bigint"
    ) {
      return String(part);
    }
    try {
      return JSON.stringify(part);
    } catch {
      return String(part);
    }
  });
  return boundedString(parts.join(" "), MAX_MESSAGE_LENGTH) ?? "";
}

function formatTimestamp(epochMs: number | null | undefined): string {
  if (typeof epochMs === "number" && Number.isFinite(epochMs)) {
    const date = new Date(epochMs);
    if (Number.isFinite(date.getTime())) {
      return date.toISOString();
    }
  }
  return new Date().toISOString();
}

function formatFlexibleTimestamp(
  value: unknown,
  fallbackEpochMs: number | null | undefined,
): string {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    const date = new Date(value);
    if (Number.isFinite(date.getTime())) {
      return date.toISOString();
    }
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Date.parse(value.trim());
    if (Number.isFinite(parsed) && parsed > 0) {
      return new Date(parsed).toISOString();
    }
  }
  return formatTimestamp(fallbackEpochMs);
}

function extractUrlMetadata(
  rawUrl: string | undefined,
  options: UrlCaptureOptions,
  captureQuery = true,
): {
  host: string | null;
  path: string | null;
  query: Record<string, string[]>;
} {
  const empty = { host: null, path: null, query: {} };
  if (!rawUrl) return empty;
  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    return empty;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return empty;

  const path = redactPathname(parsed.pathname, parsed.hostname, options);
  if (!captureQuery) return { host: parsed.hostname, path, query: {} };

  const allowed =
    options.allowedQueryParams === undefined ? null : new Set(options.allowedQueryParams);
  const query = new Map<string, string[]>();
  for (const [key, value] of parsed.searchParams) {
    if (allowed !== null && !allowed.has(key)) continue;
    if (options.redactQueryValue !== undefined) {
      const custom = options.redactQueryValue(key, value);
      if (custom === null) continue;
      const captured =
        custom !== value ? custom : redactDefaultQueryValue(key, custom, options, 0);
      const values = query.get(key) ?? [];
      values.push(captured);
      query.set(key, values);
      continue;
    }
    const captured = redactDefaultQueryValue(key, value, options, 0);
    const values = query.get(key) ?? [];
    values.push(captured);
    query.set(key, values);
  }
  return { host: parsed.hostname, path, query: Object.fromEntries(query) };
}

function redactPathname(
  rawPathname: string,
  hostname: string,
  options: UrlCaptureOptions,
): string | null {
  const pathname = options.redactPath ? options.redactPath(rawPathname, hostname) : rawPathname;
  if (pathname === null) {
    return null;
  }
  return boundedString(
    pathname
      .split("/")
      .map((segment) => {
        try {
          // Inspect decoded data before truncation, including Referer paths that Cloudflare does not redact.
          const decoded = decodeURIComponent(segment);
          return redactTokens(decoded) === decoded ? segment : "REDACTED";
        } catch {
          return "REDACTED";
        }
      })
      .join("/"),
    MAX_PATH_LENGTH,
  );
}

function redactDefaultQueryValue(
  key: string,
  value: string,
  options: UrlCaptureOptions,
  depth: number,
): string {
  if (DEFAULT_SENSITIVE_QUERY_KEYS.has(key.trim().toLowerCase())) {
    return "REDACTED";
  }
  if (value.length === 0) {
    return value;
  }

  let candidate = value;
  if (/%[0-9a-fA-F]{2}/.test(candidate)) {
    try {
      const decodedOnce = decodeURIComponent(candidate);
      if (decodedOnce !== candidate) {
        candidate = decodedOnce;
      }
    } catch {
      // Keep literal percent signs (such as invalid UTF-8 byte sequences in prose) and fall through to token redaction.
    }
  }

  const trimmed = candidate.trim();
  if (depth < 2 && /^https?:\/\//i.test(trimmed)) {
    try {
      const nested = new URL(trimmed);
      if (nested.username.length > 0) nested.username = "REDACTED";
      if (nested.password.length > 0) nested.password = "REDACTED";
      const nestedPath = redactPathname(nested.pathname, nested.hostname, options);
      nested.pathname = nestedPath ?? "/";
      if (nested.search.length > 0) {
        const sanitizedParams = new URLSearchParams();
        for (const [nestedKey, nestedVal] of nested.searchParams) {
          sanitizedParams.append(
            nestedKey,
            redactDefaultQueryValue(nestedKey, nestedVal, options, depth + 1),
          );
        }
        const qs = sanitizedParams.toString();
        nested.search = qs.length > 0 ? `?${qs}` : "";
      }
      if (nested.hash.length > 1) {
        nested.hash = `#${redactTokens(nested.hash.slice(1))}`;
      }
      return nested.toString();
    } catch {
      return redactTokens(candidate);
    }
  }

  if (depth < 2 && trimmed.startsWith("/")) {
    try {
      const isProtocolRelative = trimmed.startsWith("//");
      const nested = isProtocolRelative
        ? new URL(`https:${trimmed}`)
        : new URL(trimmed, "https://placeholder.invalid");
      const nestedPath =
        redactPathname(nested.pathname, isProtocolRelative ? nested.hostname : "", options) ?? "/";
      let querySuffix = "";
      if (nested.search.length > 0) {
        const sanitizedParams = new URLSearchParams();
        for (const [nestedKey, nestedVal] of nested.searchParams) {
          sanitizedParams.append(
            nestedKey,
            redactDefaultQueryValue(nestedKey, nestedVal, options, depth + 1),
          );
        }
        const qs = sanitizedParams.toString();
        if (qs.length > 0) {
          querySuffix = `?${qs}`;
        }
      }
      const hashSuffix =
        nested.hash.length > 1 ? `#${redactTokens(nested.hash.slice(1))}` : nested.hash;
      return isProtocolRelative
        ? `//${nested.host}${nestedPath}${querySuffix}${hashSuffix}`
        : `${nestedPath}${querySuffix}${hashSuffix}`;
    } catch {
      return redactTokens(candidate);
    }
  }

  const redactedCandidate = redactTokens(candidate);
  return redactedCandidate !== candidate ? redactedCandidate : redactTokens(value);
}

// Defense in depth: redact sensitive hex IDs, high-entropy/opaque tokens, and card numbers while preserving English route slugs.
function redactTokens(value: string): string {
  return value.replace(/[a-zA-Z0-9+_-]+/g, (token) => {
    return isSensitiveToken(token) ||
      isSensitiveToken(token.replace(/^[a-zA-Z]+[_-]/, ""))
      ? "REDACTED"
      : token;
  });
}

function isSensitiveToken(token: string): boolean {
  const withoutDate = token.replace(/^\d{4}-\d{2}-\d{2}-/, "");
  const compact = withoutDate.replace(/[+_-]/g, "");
  if (/^[a-fA-F0-9]{32,}$/.test(compact)) return true;
  if (
    withoutDate.length >= 21 &&
    (withoutDate.match(/[A-Z]/g)?.length ?? 0) >= 2 &&
    (withoutDate.match(/[a-z]/g)?.length ?? 0) >= 2 &&
    (withoutDate.match(/[0-9]/g)?.length ?? 0) >= 2
  ) {
    return true;
  }
  if (/^\d{13,19}$/.test(compact)) {
    let sum = 0;
    for (let i = compact.length - 1; i >= 0; i--) {
      let digit = Number(compact[i]);
      if ((compact.length - 1 - i) % 2 === 1) digit *= 2;
      sum += digit > 9 ? digit - 9 : digit;
    }
    if (sum % 10 === 0) return true;
  }
  return withoutDate.split(/[_-]/).some(isSensitiveSubtoken);
}

function isSensitiveSubtoken(part: string): boolean {
  if (/^[a-fA-F0-9]{12,}$/.test(part)) {
    return true;
  }
  if (part.length >= 20 && /[a-zA-Z]/.test(part) && /\d/.test(part)) {
    return true;
  }
  if (
    part.length >= 20 &&
    (/^[a-z]+$/.test(part) || /^[A-Z]+$/.test(part)) &&
    !isLikelyEnglishWord(part)
  ) {
    return true;
  }
  return false;
}

function isLikelyEnglishWord(word: string): boolean {
  if (/(.)\1\1/i.test(word)) return false;
  if (/[bcdfghjklmnpqrstvwxz]{5,}/i.test(word)) return false;
  if (/[aeiou]{4,}/i.test(word)) return false;
  const vowels = word.match(/[aeiouy]/gi)?.length ?? 0;
  const ratio = vowels / word.length;
  if (ratio < 0.3 || ratio > 0.6) return false;
  return ENGLISH_LONG_WORD_AFFIX.test(word);
}

function isPrefetchRequest(headers: Readonly<Record<string, string>> | undefined): boolean {
  if (headers === undefined) {
    return false;
  }
  if (headerValue(headers, "next-router-prefetch") === "1") {
    return true;
  }
  if (headerValue(headers, "purpose")?.toLowerCase() === "prefetch") {
    return true;
  }
  if (headerValue(headers, "sec-purpose")?.toLowerCase().includes("prefetch") === true) {
    return true;
  }
  if (headerValue(headers, "x-moz")?.toLowerCase() === "prefetch") {
    return true;
  }
  return false;
}

function extractTraceId(traceparent: string | null): string | null {
  if (traceparent === null) {
    return null;
  }
  const match = TRACEPARENT_PATTERN.exec(traceparent.trim());
  return normalizeHexId(match?.[1] ?? null, TRACE_ID_PATTERN);
}

function normalizeHexId(value: unknown, pattern: RegExp): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!pattern.test(trimmed) || /^0+$/.test(trimmed)) {
    return null;
  }
  return trimmed.toLowerCase();
}

function extractVerifiedBotCategory(
  cf: Readonly<Record<string, unknown>> | undefined,
): string | null {
  if (cf === undefined) {
    return null;
  }
  const direct = stringField(cf["verifiedBotCategory"]);
  if (direct !== null) {
    return direct;
  }
  const botManagement = cf["botManagement"];
  if (botManagement !== null && typeof botManagement === "object") {
    return stringField((botManagement as Record<string, unknown>)["verifiedBotCategory"]);
  }
  return null;
}

function safeHttpStatus(status: number | undefined): number | null {
  if (
    typeof status === "number" &&
    Number.isSafeInteger(status) &&
    status >= 100 &&
    status <= 599
  ) {
    return status;
  }
  return null;
}

function safeInteger(value: unknown): number | null {
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return value;
  }
  return null;
}

function headerValue(
  headers: Readonly<Record<string, string>> | undefined,
  targetName: string,
): string | null {
  if (headers === undefined) {
    return null;
  }
  const lowerTarget = targetName.toLowerCase();
  for (const [name, value] of Object.entries(headers)) {
    if (name.toLowerCase() === lowerTarget && typeof value === "string" && value.length > 0) {
      return value;
    }
  }
  return null;
}

function nonEmptyString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function stringField(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function boundedString(value: string | null, maxLength: number): string | null {
  if (value === null || value.length === 0) {
    return null;
  }
  return value.length <= maxLength ? value : value.slice(0, maxLength);
}

function finiteNumber(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  if (Number.isInteger(value) && !Number.isSafeInteger(value)) {
    return null;
  }
  return value;
}

// Normalize arbitrary console.log arguments so non-finite numbers, unsafe
// integers, sparse arrays, Errors, or circular objects never reject the batch.
function sanitizeValue(value: unknown, depth: number, ancestors: Set<object>): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))) {
      return String(value);
    }
    return value;
  }
  if (typeof value === "bigint") {
    return value >= MIN_INT64 && value <= MAX_INT64 ? value : value.toString();
  }
  if (value === undefined) {
    return null;
  }
  if (typeof value !== "object") {
    return String(value);
  }
  if (value instanceof Date) {
    const time = Date.prototype.getTime.call(value);
    return Number.isFinite(time) ? Date.prototype.toISOString.call(value) : "Invalid Date";
  }
  if (value instanceof Error) {
    return {
      name: value.name,
      message: value.message,
      ...(typeof value.stack === "string"
        ? { stack: boundedString(value.stack, MAX_STACK_LENGTH) }
        : {}),
    };
  }
  if (ancestors.has(value)) {
    return "[Circular]";
  }
  if (depth >= MAX_SANITIZE_DEPTH) {
    return "[MaxDepth]";
  }
  if (!Array.isArray(value) && !isPlainObject(value)) {
    return String(value);
  }

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const normalized: unknown[] = [];
      for (let i = 0; i < value.length; i += 1) {
        normalized.push(i in value ? sanitizeValue(value[i], depth + 1, ancestors) : null);
      }
      return normalized;
    }
    const output: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      output[key] = sanitizeValue(entry, depth + 1, ancestors);
    }
    return output;
  } finally {
    ancestors.delete(value);
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function requiredBinding(value: string | undefined, name: string): string {
  const trimmed = nonEmptyString(value);
  if (trimmed === null) {
    throw new Error(`Missing Worker binding: ${name}`);
  }
  return trimmed;
}
