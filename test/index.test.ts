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

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { describe, it } from "node:test";
import { gunzipSync } from "node:zlib";
import { type CatalogTable, type CatalogView, Client } from "scopedb";
import YAML from "yaml";
import {
  createTailWorker,
  type StructuredTelemetryEnvelope,
  type TailItem,
  tailItemToRows,
  tailItemsToRows,
} from "../src/index.js";

describe("cloudflare-tail-worker", () => {
  it("normalizes multi-service invocations, logs, and exceptions with default sensitive query redaction and selective header extraction", async () => {
    const requests: { url: string; headers: Headers; rawText: string; lines: Record<string, unknown>[] }[] = [];

    const worker = createTailWorker({
      fetch: async (input, init) => {
        const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
        const headers = new Headers(init?.headers);
        const rawBody = init?.body;
        assert.ok(rawBody instanceof Uint8Array || rawBody instanceof ArrayBuffer);
        const decompressed = gunzipSync(
          rawBody instanceof Uint8Array ? rawBody : new Uint8Array(rawBody),
        ).toString("utf8");
        const lines = decompressed
          .trim()
          .split("\n")
          .filter((line) => line.length > 0)
          .map((line) => JSON.parse(line) as Record<string, unknown>);
        requests.push({ url, headers, rawText: decompressed, lines });
        return Response.json({ num_rows_inserted: lines.length });
      },
    });

    const circular: Record<string, unknown> = { stage: "upstream" };
    circular["self"] = circular;

    const events: TailItem[] = [
      {
        scriptName: "scopedb-site",
        outcome: "ok",
        eventTimestamp: 1730620800000,
        wallTime: 18.5,
        cpuTime: 3.2,
        event: {
          request: {
            url: "https://www.scopedb.io/docs/guides/sdks/nodejs.md?utm_source=hackernews&utm_medium=social&ref=launch&landing_page=/blog/scopedb-cloud-public-preview/&from_page=/agent&token=secret-query#section",
            method: "GET",
            headers: {
              "CF-Ray": "8dc1234567890abc-SJC",
              "User-Agent": "ClaudeBot/1.0 (+https://www.anthropic.com)",
              Accept: "text/markdown, text/plain;q=0.9",
              "Sec-Fetch-Mode": "navigate",
              "Sec-Fetch-Dest": "document",
              traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
              Referer: "https://news.ycombinator.com/item?id=42000000&secret=123",
              Authorization: "Bearer super-secret-user-token",
              Cookie: "session=private-cookie",
            },
            cf: {
              colo: "SJC",
              continent: "NA",
              country: "US",
              region: "California",
              city: "San Jose",
              timezone: "America/Los_Angeles",
              asn: 399358,
              asOrganization: "Anthropic, PBC",
              httpProtocol: "HTTP/2",
              tlsVersion: "TLSv1.3",
              botManagement: {
                verifiedBotCategory: "AI Crawler",
              },
            },
          },
          response: {
            status: 200,
          },
        },
        logs: [
          {
            timestamp: 1730620800010,
            level: "log",
            message: [
              "served markdown doc",
              {
                safe_bigint: BigInt("9007199254740993"),
                overflow_bigint: BigInt("9223372036854775808"),
                unsafe_int: 9007199254740992,
                non_finite: Number.NaN,
                circular,
              },
            ],
          },
        ],
      },
      {
        scriptName: "scopedb-console",
        outcome: "exception",
        eventTimestamp: 1730620805000,
        wallTime: 142.0,
        cpuTime: 11.4,
        truncated: true,
        event: {
          request: {
            url: "https://console.scopedb.cloud/reset/tok_a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1?workspace=ws_123&session_id=opaque-value",
            method: "POST",
            headers: {
              "cf-ray": "8dc9999999999def-FRA",
              "Next-Router-Prefetch": "1",
              RSC: "1",
            },
            cf: {
              colo: "FRA",
              country: "DE",
            },
          },
          response: {
            status: 502,
          },
        },
        exceptions: [
          {
            timestamp: 1730620805100,
            name: "TypeError",
            message: "Upstream gateway timeout",
            stack: "TypeError: Upstream gateway timeout\n    at fetchUpstream (worker.js:42:11)",
          },
        ],
      },
    ];

    await worker.tail(events, {
      SCOPEDB_ENDPOINT: "https://workspace.scopedb.cloud",
      SCOPEDB_API_KEY: "test-api-key",
    });

    assert.equal(requests.length, 1);
    const req = requests[0]!;
    assert.equal(
      req.url,
      "https://workspace.scopedb.cloud/v1/catalog/databases/scopedb/schemas/public/tables/cloudflare_worker_events/ingest",
    );
    assert.equal(req.headers.get("authorization"), "Bearer test-api-key");
    assert.equal(req.headers.get("content-encoding"), "gzip");
    assert.equal(req.headers.get("content-type"), "application/x-ndjson");
    assert.equal(req.lines.length, 4);
    assert.equal(req.rawText.includes('"safe_bigint":9007199254740993'), true);

    const [siteInvocation, siteLog, consoleInvocation, consoleException] = req.lines as [
      Record<string, unknown>,
      Record<string, unknown>,
      Record<string, unknown>,
      Record<string, unknown>,
    ];

    assert.equal(siteInvocation["service"], "scopedb-site");
    assert.equal(siteInvocation["event_type"], "worker.invocation");
    assert.equal(siteInvocation["outcome"], "ok");
    assert.equal(siteInvocation["level"], "info");
    assert.equal(siteInvocation["method"], "GET");
    assert.equal(siteInvocation["host"], "www.scopedb.io");
    assert.equal(siteInvocation["path"], "/docs/guides/sdks/nodejs.md");
    assert.equal(siteInvocation["status"], 200);
    assert.equal(siteInvocation["ray_id"], "8dc1234567890abc-SJC");
    assert.equal(siteInvocation["message"], "GET /docs/guides/sdks/nodejs.md -> 200");

    const siteAttrs = siteInvocation["attributes"] as Record<string, unknown>;
    assert.equal(siteAttrs["user_agent"], "ClaudeBot/1.0 (+https://www.anthropic.com)");
    assert.equal(siteAttrs["accept"], "text/markdown, text/plain;q=0.9");
    assert.equal(siteAttrs["sec_fetch_mode"], "navigate");
    assert.equal(siteAttrs["sec_fetch_dest"], "document");
    assert.equal(siteAttrs["is_prefetch"], false);
    assert.equal(siteAttrs["is_rsc"], false);
    assert.equal(siteAttrs["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
    assert.equal(siteAttrs["referer_host"], "news.ycombinator.com");
    assert.equal(siteAttrs["referer_path"], "/item");
    assert.deepEqual(siteAttrs["query"], {
      utm_source: ["hackernews"],
      utm_medium: ["social"],
      ref: ["launch"],
      landing_page: ["/blog/scopedb-cloud-public-preview/"],
      from_page: ["/agent"],
      token: ["REDACTED"],
    });
    assert.equal(siteAttrs["colo"], "SJC");
    assert.equal(siteAttrs["continent"], "NA");
    assert.equal(siteAttrs["country"], "US");
    assert.equal(siteAttrs["region"], "California");
    assert.equal(siteAttrs["city"], "San Jose");
    assert.equal(siteAttrs["timezone"], "America/Los_Angeles");
    assert.equal(siteAttrs["asn"], 399358);
    assert.equal(siteAttrs["as_organization"], "Anthropic, PBC");
    assert.equal(siteAttrs["verified_bot_category"], "AI Crawler");

    assert.equal(siteLog["service"], "scopedb-site");
    assert.equal(siteLog["event_type"], "worker.log");
    assert.equal(siteLog["level"], "info");
    const logAttrs = siteLog["attributes"] as Record<string, unknown>;
    assert.equal(logAttrs["invocation_id"], siteInvocation["event_id"]);
    assert.equal(logAttrs["truncated"], false);
    const args = logAttrs["arguments"] as unknown[];
    const meta = args[1] as Record<string, unknown>;
    assert.equal(meta["overflow_bigint"], "9223372036854775808");
    assert.equal(meta["unsafe_int"], "9007199254740992");
    assert.equal(meta["non_finite"], "NaN");
    assert.deepEqual(meta["circular"], { stage: "upstream", self: "[Circular]" });

    assert.equal(consoleInvocation["service"], "scopedb-console");
    assert.equal(consoleInvocation["event_type"], "worker.invocation");
    assert.equal(consoleInvocation["level"], "error");
    assert.equal(consoleInvocation["host"], "console.scopedb.cloud");
    assert.equal(consoleInvocation["path"], "/reset/REDACTED");
    assert.equal(consoleInvocation["status"], 502);
    const consoleAttrs = consoleInvocation["attributes"] as Record<string, unknown>;
    assert.equal(consoleAttrs["is_prefetch"], true);
    assert.equal(consoleAttrs["is_rsc"], true);
    assert.equal(consoleAttrs["truncated"], true);
    assert.deepEqual(consoleAttrs["query"], {
      workspace: ["ws_123"],
      session_id: ["REDACTED"],
    });

    assert.equal(consoleException["service"], "scopedb-console");
    assert.equal(consoleException["event_type"], "worker.exception");
    assert.equal(consoleException["level"], "error");
    assert.equal(consoleException["message"], "TypeError: Upstream gateway timeout");
    const excAttrs = consoleException["attributes"] as Record<string, unknown>;
    assert.equal(excAttrs["invocation_id"], consoleInvocation["event_id"]);
    assert.equal(excAttrs["exception_name"], "TypeError");
    assert.equal(excAttrs["truncated"], true);

    const serializedBatch = JSON.stringify(req.lines);
    assert.ok(!serializedBatch.includes("super-secret-user-token"));
    assert.ok(!serializedBatch.includes("private-cookie"));
    assert.ok(!serializedBatch.includes("secret=123"));
    assert.ok(!serializedBatch.includes("secret-query"));
    assert.ok(!serializedBatch.includes("opaque-value"));
  });

  it("treats client-canceled outcomes as info rather than error", () => {
    const [canceled] = tailItemToRows({
      scriptName: "scopedb-site",
      outcome: "canceled",
      event: { request: { url: "https://www.scopedb.io/docs/guides", method: "GET" } },
    });
    assert.equal(canceled!.outcome, "canceled");
    assert.equal(canceled!.level, "info");

    const [exceeded] = tailItemToRows({
      scriptName: "scopedb-site",
      outcome: "exceededCpu",
      event: { request: { url: "https://www.scopedb.io/docs/guides", method: "GET" } },
    });
    assert.equal(exceeded!.level, "error");
  });

  it("supports custom shouldRecord filtering and validates required bindings", async () => {
    let fetchCalls = 0;
    const worker = createTailWorker({
      shouldRecord: (item) => item.outcome !== "ok",
      fetch: async () => {
        fetchCalls += 1;
        return new Response("{}", { status: 200 });
      },
    });

    await worker.tail(
      [{ scriptName: "scopedb-site", outcome: "ok" }],
      { SCOPEDB_ENDPOINT: "https://workspace.scopedb.cloud", SCOPEDB_API_KEY: "key" },
    );
    assert.equal(fetchCalls, 0);

    await assert.rejects(
      () =>
        worker.tail(
          [{ scriptName: "scopedb-site", outcome: "exception" }],
          { SCOPEDB_ENDPOINT: "", SCOPEDB_API_KEY: "key" },
        ),
      /Missing Worker binding: SCOPEDB_ENDPOINT/,
    );

    const rows = tailItemsToRows([{ outcome: "ok" }]);
    assert.equal(rows.length, 1);
    assert.equal(rows[0]?.service, "unknown");
    assert.equal(rows[0]?.message, "invocation (ok)");
  });

  it("preserves decoded non-secret query values and duplicates while redacting sensitive keys, tokens, and embedded paths", () => {
    const params = new URLSearchParams();
    for (let i = 0; i < 40; i++) params.append("tag", `value-${i}`);
    params.append("utm_source", "first");
    params.append("utm_source", "last");
    params.append("payload", "search term ".repeat(350));
    params.append("email", "alice@example.com");
    params.append("token", "tok_" + "ab12".repeat(16));
    params.append("Code", "gho_short20charhex1234");
    params.append("X-Amz-Signature", "abcdef0123456789");
    params.append("returnTo", "/invitations/aaaaaaaaaaaaaaaaaaaaaaaa?state=xyz");
    params.append("doubleEncoded", "%2Finvitations%2Faaaaaaaaaaaaaaaaaaaaaaaa");
    params.append("percentText", "100% ready");
    params.append("redirect", "https://console.scopedb.cloud/workspaces/777257d6587f/settings?invite=secret");
    params.append("embedded_tok", "tok_" + "ab12".repeat(16));
    params.append("__proto__", "ordinary query key");
    params.append("empty", "");

    const [row] = tailItemToRows({
      outcome: "ok",
      event: { request: { url: `https://example.com/docs/guides/analyze-cloudflare-worker-traffic?${params}` } },
    });
    assert.equal(row!.path, "/docs/guides/analyze-cloudflare-worker-traffic");
    const query = row!.attributes["query"] as Record<string, string[]>;
    assert.equal(query["tag"]?.length, 40);
    assert.deepEqual(query["utm_source"], ["first", "last"]);
    assert.equal(query["payload"]?.[0], "search term ".repeat(350));
    assert.deepEqual(query["email"], ["alice@example.com"]);
    assert.deepEqual(query["token"], ["REDACTED"]);
    assert.deepEqual(query["Code"], ["REDACTED"]);
    assert.deepEqual(query["X-Amz-Signature"], ["REDACTED"]);
    assert.deepEqual(query["returnTo"], ["/invitations/REDACTED?state=REDACTED"]);
    assert.deepEqual(query["doubleEncoded"], ["/invitations/REDACTED"]);
    assert.deepEqual(query["percentText"], ["100% ready"]);
    assert.deepEqual(query["redirect"], [
      "https://console.scopedb.cloud/workspaces/REDACTED/settings?invite=REDACTED",
    ]);
    assert.deepEqual(query["embedded_tok"], ["REDACTED"]);
    assert.deepEqual(query["__proto__"], ["ordinary query key"]);
    assert.deepEqual(query["empty"], [""]);
  });

  it("redacts 12-char hex IDs, 20-char opaque tokens, and prefixed credentials while preserving English route slugs", () => {
    for (const token of [
      "777257d6587f",
      "6ef0e720a1e3",
      "aaaaaaaaaaaaaaaaaaaaaaaa",
      "01h455vb4pex5vsknk084sn02q",
      "ab12".repeat(16),
      "tok_" + "ab12".repeat(16),
      "tok_12345678-abcd-1234-abcd-123456789abc",
      "AbCd12EfGh34IjKl56MnOp",
      "4111-1111-1111-1111",
    ]) {
      const encoded = [...token].map((char) => `%${char.charCodeAt(0).toString(16)}`).join("");
      const rows = tailItemToRows({
        outcome: "exception",
        event: {
          request: {
            url: `https://example.com/invitations/${encoded}`,
            headers: { referer: `https://example.com/invitations/${token}?secret=hidden` },
          },
        },
        logs: [{ level: "info", message: ["request failed"] }],
        exceptions: [{ name: "Error", message: "failed" }],
      });
      assert.ok(rows.every((row) => row.path === "/invitations/REDACTED"), `failed to redact ${token}`);
      assert.equal(rows[0]!.attributes["referer_path"], "/invitations/REDACTED");
      assert.ok(!JSON.stringify(rows).includes(token));
    }

    for (const safePath of [
      "/docs/guides/analyze-cloudflare-worker-traffic",
      "/blog/2026-10-23-javascript-sdk-0-3-0",
      "/docs/reference/internationalization",
      "/components/StaticAssetsIncrementalCache",
      "/tables/cloudflare_worker_events",
    ]) {
      const [row] = tailItemToRows({
        outcome: "ok",
        event: { request: { url: `https://www.scopedb.io${safePath}` } },
      });
      assert.equal(row!.path, safePath);
    }
  });

  it("applies optional URL capture policy before sending records", async () => {
    const worker = createTailWorker({
      allowedQueryParams: ["tag", "code"],
      redactQueryValue: (key, value) => key === "code" ? null : value.toUpperCase(),
      redactPath: (path) => path.startsWith("/reset/") ? "/reset/:token" : path,
      fetch: async (_input, init) => {
        const rows = gunzipSync(init!.body as Uint8Array).toString().trim().split("\n").map((line) => JSON.parse(line));
        assert.deepEqual(rows[0].attributes.query, { tag: ["A", "B"] });
        assert.equal(rows[0].path, "/reset/:token");
        assert.equal(rows[0].attributes.referer_path, "/reset/:token");
        assert.ok(!JSON.stringify(rows).includes("short-secret"));
        return Response.json({ num_rows_inserted: rows.length });
      },
    });
    await worker.tail([{
      outcome: "ok",
      event: { request: {
        url: "https://example.com/reset/short-secret?tag=a&tag=b&code=short-secret&email=alice%40example.com",
        headers: { referer: "https://example.com/reset/short-secret" },
      } },
    }], { SCOPEDB_ENDPOINT: "https://example.invalid", SCOPEDB_API_KEY: "test" });
    const [omitted] = tailItemToRows({ outcome: "ok", event: { request: { url: "https://example.com/reset/short-secret?tag=a" } } }, {
      allowedQueryParams: [], redactPath: () => null,
    });
    assert.equal(omitted!.path, null);
    assert.deepEqual(omitted!.attributes["query"], {});
  });

  it("parses structured producer telemetry envelopes (object and JSON string), redacts paths/queries, and blocks worker.* spoofing", () => {
    const objectEnvelope: StructuredTelemetryEnvelope = {
      __scopedb_telemetry: 1,
      environment: "production",
      app_version: "1.4.2",
      events: [
        {
          event_type: "rum.pageview",
          occurred_at: "2026-11-14T12:00:01.250Z",
          path: "/invitations/aaaaaaaaaaaaaaaaaaaaaaaa?utm_source=producthunt&token=secret-tok",
          referrer: "https://www.producthunt.com/posts/scopedb?secret=ref-secret",
          trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
          attributes: {
            session_id: "sess-anon-123",
            view_id: "view-456",
            colo: "SPOOFED_COLO",
            invocation_id: "SPOOFED_INVOCATION",
          },
        },
        {
          event_type: "rum.vital",
          path: "/docs/guides/analyze-cloudflare-worker-traffic",
          attributes: {
            session_id: "sess-anon-123",
            vital_name: "LCP",
            vital_value: 712.4,
            vital_rating: "good",
          },
        },
        {
          event_type: "rum.error",
          path: "/workspaces/777257d6587f",
          message: "Unhandled rejection: ChunkLoadError",
          attributes: {
            error_type: "ChunkLoadError",
            stack: "ChunkLoadError: Loading chunk failed\n    at app.js:10:4",
          },
        },
        {
          // Attempted worker.* spoofing must be dropped.
          event_type: "worker.invocation" as "app.invalid",
          message: "spoofed",
        },
      ],
    };

    const jsonStringSpanEnvelope = JSON.stringify({
      __scopedb_telemetry: 1,
      event: {
        event_type: "trace.span",
        occurred_at: 1730620800500,
        span_id: "00f067aa0ba902b7",
        parent_span_id: "1111222233334444",
        duration_ms: 48.7,
        attributes: {
          span_name: "POST /api/query",
          span_kind: "SERVER",
          status_code: "ERROR",
          "http.request.method": "POST",
          "http.route": "/api/query",
          "http.response.status_code": 502,
          "error.type": "timeout",
          "scopedb.failure.stage": "upstream_transport",
          "scopedb.request.outcome": "failed",
        },
      },
    });

    const rows = tailItemToRows({
      scriptName: "scopedb-console-staging",
      scriptVersion: { id: "ver-abc-123" },
      outcome: "ok",
      truncated: true,
      eventTimestamp: 1730620800000,
      event: {
        request: {
          url: "https://console-staging.scopedb.cloud/api/telemetry",
          method: "POST",
          headers: {
            "cf-ray": "8dc5555555555abc-SEA",
            "user-agent": "Mozilla/5.0 TestBrowser",
            traceparent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1-bbbbbbbbbbbbbbb2-01",
          },
          cf: {
            colo: "SEA",
            country: "US",
            region: "Washington",
            city: "Seattle",
            continent: "NA",
            timezone: "America/Los_Angeles",
            asn: 13335,
            asOrganization: "Cloudflare, Inc.",
          },
        },
        response: { status: 204 },
      },
      logs: [
        { timestamp: 1730620800100, level: "info", message: [objectEnvelope] },
        { timestamp: 1730620800200, level: "info", message: [jsonStringSpanEnvelope] },
        { timestamp: 1730620800300, level: "info", message: ["ordinary log line"] },
      ],
    });

    // 1 worker.invocation + 3 valid RUM events (spoofed worker.invocation dropped) + 1 trace.span + 1 ordinary worker.log = 6 rows.
    assert.equal(rows.length, 6);
    const [inv, pageview, vital, rumErr, span, plainLog] = rows;

    assert.equal(inv!.event_type, "worker.invocation");
    assert.equal(inv!.attributes["log_count"], 1);
    assert.equal(inv!.attributes["truncated"], true);

    assert.equal(pageview!.event_type, "rum.pageview");
    assert.equal(pageview!.occurred_at, "2026-11-14T12:00:01.250Z");
    assert.equal(pageview!.service, "scopedb-console-staging");
    assert.equal(pageview!.method, null);
    assert.equal(pageview!.status, null);
    assert.equal(pageview!.host, "console-staging.scopedb.cloud");
    assert.equal(pageview!.path, "/invitations/REDACTED");
    assert.equal(pageview!.level, "info");
    assert.equal(pageview!.outcome, "ok");
    assert.equal(pageview!.attributes["colo"], "SEA");
    assert.equal(pageview!.attributes["invocation_id"], inv!.event_id);
    assert.equal(pageview!.attributes["truncated"], true);
    assert.equal(pageview!.attributes["environment"], "production");
    assert.equal(pageview!.attributes["app_version"], "1.4.2");
    assert.equal(pageview!.attributes["referer_host"], "www.producthunt.com");
    assert.equal(pageview!.attributes["referer_path"], "/posts/scopedb");
    assert.deepEqual(pageview!.attributes["query"], {
      utm_source: ["producthunt"],
      token: ["REDACTED"],
    });
    assert.ok(!JSON.stringify(pageview).includes("ref-secret"));
    assert.ok(!JSON.stringify(pageview).includes("secret-tok"));

    assert.equal(vital!.event_type, "rum.vital");
    assert.equal(vital!.message, "LCP 712.4");
    assert.equal(vital!.attributes["vital_value"], 712.4);

    assert.equal(rumErr!.event_type, "rum.error");
    assert.equal(rumErr!.level, "error");
    assert.equal(rumErr!.outcome, "error");
    assert.equal(rumErr!.path, "/workspaces/REDACTED");

    assert.equal(span!.event_type, "trace.span");
    assert.equal(span!.level, "error");
    assert.equal(span!.outcome, "error");
    assert.equal(span!.method, "POST");
    assert.equal(span!.path, "/api/query");
    assert.equal(span!.status, 502);
    assert.equal(span!.attributes["trace_id"], "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1");
    assert.equal(span!.attributes["span_id"], "00f067aa0ba902b7");
    assert.equal(span!.attributes["parent_span_id"], "1111222233334444");
    assert.equal(span!.attributes["duration_ms"], 48.7);
    assert.equal(span!.attributes["environment"], "staging");
    assert.equal(span!.attributes["script_version_id"], "ver-abc-123");
    assert.equal(span!.attributes["truncated"], true);

    assert.equal(plainLog!.event_type, "worker.log");
    assert.equal(plainLog!.message, "ordinary log line");
    assert.equal(plainLog!.attributes["truncated"], true);
  });

  it("bounds exception stacks and propagates upstream truncation signals across all event types", () => {
    const [invocation, logRow, exception] = tailItemToRows({
      outcome: "exception",
      truncated: true,
      logs: [{ level: "error", message: ["partial log"] }],
      exceptions: [{ name: "Error", message: "failed", stack: "x".repeat(10000) }],
    });
    assert.equal(invocation!.attributes["truncated"], true);
    assert.equal(logRow!.attributes["truncated"], true);
    assert.equal(exception!.attributes["truncated"], true);
    assert.equal((exception!.attributes["stack"] as string).length, 8192);
  });

  it("reports uncertain delivery without replaying a possibly committed batch", async () => {
    let calls = 0;
    const worker = createTailWorker({ fetch: async () => { calls++; throw new TypeError("connection lost"); } });
    await assert.rejects(worker.tail([{ outcome: "ok" }], {
      SCOPEDB_ENDPOINT: "https://example.invalid", SCOPEDB_API_KEY: "test",
    }));
    assert.equal(calls, 1);
  });

  it("serializes all six Catalog definitions through the SDK and aligns table columns with all row types", async () => {
    const tableDef = YAML.parse(
      await readFile(new URL("../../catalog/table.yaml", import.meta.url), "utf8"),
    ) as CatalogTable;

    const client = new Client("https://workspace.scopedb.cloud", {
      apiKey: "test-key",
      fetch: async (_input, init) => {
        const body = JSON.parse(String(init?.body)) as unknown;
        return new Response(
          JSON.stringify({ before: null, after: body, destructive: false }),
          {
            status: 200,
            headers: { "Content-Type": "application/json", ETag: '"v1"' },
          },
        );
      },
    });

    const tableResult = await client
      .catalog()
      .table("scopedb", "public", "cloudflare_worker_events")
      .apply(tableDef);
    assert.equal(tableResult.after?.name, "cloudflare_worker_events");

    const expectedColumns = (tableDef.columns ?? []).map((col) => col.name);
    const sampleRows = tailItemsToRows([
      {
        scriptName: "scopedb-site",
        outcome: "ok",
        logs: [
          { level: "info", message: ["ok"] },
          {
            level: "info",
            message: [
              {
                __scopedb_telemetry: 1,
                events: [
                  { event_type: "rum.pageview", path: "/docs" },
                  { event_type: "trace.span", span_id: "00f067aa0ba902b7", duration_ms: 12.3 },
                  { event_type: "app.webhook_received", attributes: { provider: "github" } },
                ],
              },
            ],
          },
        ],
        exceptions: [{ name: "Error", message: "boom" }],
      },
    ]);
    assert.equal(sampleRows.length, 6);
    for (const row of sampleRows) {
      assert.deepEqual(Object.keys(row), expectedColumns);
    }

    for (const viewName of [
      "v_cloudflare_requests",
      "v_cloudflare_pageviews",
      "v_cloudflare_errors",
      "v_cloudflare_ai_bots",
      "v_cloudflare_rum",
      "v_cloudflare_traces",
    ]) {
      const viewDef = YAML.parse(
        await readFile(new URL(`../../catalog/${viewName}.yaml`, import.meta.url), "utf8"),
      ) as CatalogView;
      const viewResult = await client.catalog().view("scopedb", "public", viewName).apply(viewDef);
      assert.equal(viewResult.after?.name, viewName);
    }
  });
});
