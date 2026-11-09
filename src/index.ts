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

export interface TailItem {
  readonly scriptName?: string | null;
  readonly outcome: string;
  readonly eventTimestamp?: number | null;
  readonly wallTime?: number;
  readonly cpuTime?: number;
  readonly truncated?: boolean;
  readonly event?: TailRequestEvent | null;
  readonly logs?: readonly TailLog[];
  readonly exceptions?: readonly TailException[];
}

export interface CloudflareWorkerEventRow extends Record<string, unknown> {
  event_id: string;
  occurred_at: string;
  service: string;
  event_type: "worker.invocation" | "worker.log" | "worker.exception";
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
  /** Capture only these query keys. By default all keys are captured; [] disables query capture. */
  readonly allowedQueryParams?: readonly string[];
  /** Applied to each decoded query value. Return null to omit that value. */
  readonly redactQueryValue?: (key: string, value: string) => string | null;
  /** Applied to request and Referer pathnames before token redaction. Return null to omit a path. */
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

export function tailItemsToRows(events: readonly TailItem[], options: UrlCaptureOptions = {}): CloudflareWorkerEventRow[] {
  return events.flatMap((item) => tailItemToRows(item, options));
}

export function tailItemToRows(item: TailItem, options: UrlCaptureOptions = {}): CloudflareWorkerEventRow[] {
  const invocationId = crypto.randomUUID();
  const occurredAt = formatTimestamp(item.eventTimestamp);
  const service = nonEmptyString(item.scriptName) ?? "unknown";
  const outcome = nonEmptyString(item.outcome) ?? "unknown";
  const request = item.event?.request;
  const method = nonEmptyString(request?.method);
  const { host, path, query } = extractUrlMetadata(request?.url, options);
  const referer = extractUrlMetadata(headerValue(request?.headers, "referer") ?? undefined, options, false);
  const status = safeHttpStatus(item.event?.response?.status);
  const rayId = headerValue(request?.headers, "cf-ray");
  const logs = item.logs ?? [];
  const exceptions = item.exceptions ?? [];

  const rows: CloudflareWorkerEventRow[] = [
    {
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
        log_count: logs.length,
        exception_count: exceptions.length,
        truncated: item.truncated ?? false,
        user_agent: boundedString(headerValue(request?.headers, "user-agent"), MAX_USER_AGENT_LENGTH),
        accept: boundedString(headerValue(request?.headers, "accept"), MAX_ACCEPT_LENGTH),
        sec_fetch_mode: stringField(headerValue(request?.headers, "sec-fetch-mode")),
        sec_fetch_dest: stringField(headerValue(request?.headers, "sec-fetch-dest")),
        is_prefetch: isPrefetchRequest(request?.headers),
        is_rsc: headerValue(request?.headers, "rsc") === "1",
        trace_id: extractTraceId(headerValue(request?.headers, "traceparent")),
        referer_host: referer.host,
        referer_path: referer.path,
        query,
        colo: stringField(request?.cf?.["colo"]),
        continent: stringField(request?.cf?.["continent"]),
        country: stringField(request?.cf?.["country"]),
        region: stringField(request?.cf?.["region"]),
        city: stringField(request?.cf?.["city"]),
        timezone: stringField(request?.cf?.["timezone"]),
        asn: safeInteger(request?.cf?.["asn"]),
        as_organization: stringField(request?.cf?.["asOrganization"]),
        http_protocol: stringField(request?.cf?.["httpProtocol"]),
        tls_version: stringField(request?.cf?.["tlsVersion"]),
        verified_bot_category: extractVerifiedBotCategory(request?.cf),
      },
    },
  ];

  for (const log of logs) {
    const sanitizedArgs = sanitizeValue(log.message, 0, new Set());
    rows.push({
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
        arguments: sanitizedArgs,
      },
    });
  }

  for (const exception of exceptions) {
    const exceptionName = nonEmptyString(exception.name) ?? "Error";
    const exceptionMessage = typeof exception.message === "string" ? exception.message : "";
    rows.push({
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
      message: boundedString(
        exceptionMessage.length > 0 ? `${exceptionName}: ${exceptionMessage}` : exceptionName,
        MAX_MESSAGE_LENGTH,
      ) ?? exceptionName,
      attributes: {
        invocation_id: invocationId,
        exception_name: exceptionName,
        stack: boundedString(exception.stack ?? null, MAX_STACK_LENGTH),
      },
    });
  }

  return rows;
}

function invocationLevel(
  outcome: string,
  status: number | null,
  exceptionCount: number,
): "info" | "warn" | "error" {
  if (outcome !== "ok" || exceptionCount > 0 || (status !== null && status >= 500)) {
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
    if (part === null || typeof part === "number" || typeof part === "boolean" || typeof part === "bigint") {
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

function extractUrlMetadata(rawUrl: string | undefined, options: UrlCaptureOptions, captureQuery = true): {
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

  const pathname = options.redactPath ? options.redactPath(parsed.pathname, parsed.hostname) : parsed.pathname;
  const path = pathname === null ? null : boundedString(pathname.split("/").map((segment) => {
    try {
      // Inspect decoded data before truncation, including Referer paths that Cloudflare does not redact.
      const decoded = decodeURIComponent(segment);
      return redactTokens(decoded) === decoded ? segment : "REDACTED";
    } catch {
      return "REDACTED";
    }
  }).join("/"), MAX_PATH_LENGTH);

  if (!captureQuery) return { host: parsed.hostname, path, query: {} };
  const allowed = options.allowedQueryParams === undefined ? null : new Set(options.allowedQueryParams);
  const query = new Map<string, string[]>();
  for (const [key, value] of parsed.searchParams) {
    if (allowed !== null && !allowed.has(key)) continue;
    const captured = options.redactQueryValue ? options.redactQueryValue(key, value) : value;
    if (captured === null) continue;
    const values = query.get(key) ?? [];
    values.push(captured);
    query.set(key, values);
  }
  return { host: parsed.hostname, path, query: Object.fromEntries(query) };
}

// Defense in depth only: short secrets and personal data require an application capture policy.
function redactTokens(value: string): string {
  return value.replace(/[a-zA-Z0-9+_-]+/g, (token) => {
    // Prefixes such as tok_ can hide an otherwise recognizable hex identifier.
    return isSensitiveToken(token)
      || isSensitiveToken(token.replace(/^[a-zA-Z]+[_-]/, ""))
      || token.split(/[_-]/).some(isSensitiveToken) ? "REDACTED" : token;
  });
}

function isSensitiveToken(token: string): boolean {
  const compact = token.replace(/[+_-]/g, "");
  if (/^[a-fA-F0-9]{32,}$/.test(compact)) return true;
  if (token.length >= 21 && (token.match(/[A-Z]/g)?.length ?? 0) >= 2
    && (token.match(/[a-z]/g)?.length ?? 0) >= 2 && (token.match(/[0-9]/g)?.length ?? 0) >= 2) return true;
  if (!/^\d{13,19}$/.test(compact)) return false;
  let sum = 0;
  for (let i = compact.length - 1; i >= 0; i--) {
    let digit = Number(compact[i]);
    if ((compact.length - 1 - i) % 2 === 1) digit *= 2;
    sum += digit > 9 ? digit - 9 : digit;
  }
  return sum % 10 === 0;
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

const TRACEPARENT_PATTERN = /^00-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$/i;

function extractTraceId(traceparent: string | null): string | null {
  if (traceparent === null) {
    return null;
  }
  const match = TRACEPARENT_PATTERN.exec(traceparent.trim());
  const traceId = match?.[1]?.toLowerCase() ?? null;
  if (traceId === null || traceId === "00000000000000000000000000000000") {
    return null;
  }
  return traceId;
}

function extractVerifiedBotCategory(cf: Readonly<Record<string, unknown>> | undefined): string | null {
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
  if (typeof status === "number" && Number.isSafeInteger(status) && status >= 100 && status <= 599) {
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
function sanitizeValue(
  value: unknown,
  depth: number,
  ancestors: Set<object>,
): unknown {
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
      ...(typeof value.stack === "string" ? { stack: boundedString(value.stack, MAX_STACK_LENGTH) } : {}),
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
      return Array.from(value, (item) => sanitizeValue(item, depth + 1, ancestors));
    }
    const sanitized: Record<string, unknown> = {};
    for (const key of Object.keys(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || !("value" in descriptor) || descriptor.value === undefined) {
        continue;
      }
      sanitized[key] = sanitizeValue(descriptor.value, depth + 1, ancestors);
    }
    return sanitized;
  } finally {
    ancestors.delete(value);
  }
}

function isPlainObject(value: object): value is Record<string, unknown> {
  const prototype: unknown = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function requiredBinding(value: unknown, name: string): string {
  const normalized = nonEmptyString(value);
  if (normalized === null) {
    throw new Error(`Missing Worker binding: ${name}`);
  }
  return normalized;
}
