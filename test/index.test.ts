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
import { createTailWorker, type TailItem, tailItemsToRows } from "../src/index.js";

describe("cloudflare-tail-worker", () => {
  it("normalizes multi-service invocations, logs, and exceptions while redacting sensitive URLs and headers", async () => {
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
        event: {
          request: {
            url: "https://console.scopedb.cloud/invitations/REDACTED?workspace=ws_123&session_id=sess_secret_999",
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
    assert.equal(siteAttrs["utm_source"], "hackernews");
    assert.equal(siteAttrs["utm_medium"], "social");
    assert.equal(siteAttrs["utm_campaign"], null);
    assert.equal(siteAttrs["utm_term"], null);
    assert.equal(siteAttrs["utm_content"], null);
    assert.equal(siteAttrs["ref"], "launch");
    assert.deepEqual(siteAttrs["query"], {
      utm_source: "hackernews",
      utm_medium: "social",
      ref: "launch",
      landing_page: "/blog/scopedb-cloud-public-preview/",
      from_page: "/agent",
      token: "REDACTED",
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
    assert.equal(consoleInvocation["path"], "/invitations/REDACTED");
    assert.equal(consoleInvocation["status"], 502);
    const consoleAttrs = consoleInvocation["attributes"] as Record<string, unknown>;
    assert.equal(consoleAttrs["is_prefetch"], true);
    assert.equal(consoleAttrs["is_rsc"], true);
    assert.deepEqual(consoleAttrs["query"], {
      workspace: "ws_123",
      session_id: "REDACTED",
    });

    assert.equal(consoleException["service"], "scopedb-console");
    assert.equal(consoleException["event_type"], "worker.exception");
    assert.equal(consoleException["level"], "error");
    assert.equal(consoleException["message"], "TypeError: Upstream gateway timeout");
    const excAttrs = consoleException["attributes"] as Record<string, unknown>;
    assert.equal(excAttrs["invocation_id"], consoleInvocation["event_id"]);
    assert.equal(excAttrs["exception_name"], "TypeError");

    const serializedBatch = JSON.stringify(req.lines);
    assert.ok(!serializedBatch.includes("secret-query"));
    assert.ok(!serializedBatch.includes("super-secret-user-token"));
    assert.ok(!serializedBatch.includes("private-cookie"));
    assert.ok(!serializedBatch.includes("secret=123"));
    assert.ok(!serializedBatch.includes("sess_secret_999"));
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

  it("validates Catalog YAML table and view definitions against the ScopeDB SDK and emitted row columns", async () => {
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
        logs: [{ level: "info", message: ["ok"] }],
        exceptions: [{ name: "Error", message: "boom" }],
      },
    ]);
    for (const row of sampleRows) {
      assert.deepEqual(Object.keys(row), expectedColumns);
    }

    for (const viewName of [
      "v_cloudflare_requests",
      "v_cloudflare_pageviews",
      "v_cloudflare_errors",
      "v_cloudflare_ai_bots",
    ]) {
      const viewDef = YAML.parse(
        await readFile(new URL(`../../catalog/${viewName}.yaml`, import.meta.url), "utf8"),
      ) as CatalogView;
      const viewResult = await client.catalog().view("scopedb", "public", viewName).apply(viewDef);
      assert.equal(viewResult.after?.name, viewName);
    }
  });
});
