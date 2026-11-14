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
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { it } from "node:test";
import { Client, type CatalogTable, type CatalogView } from "scopedb";
import YAML from "yaml";
import { tailItemToRows, type TailItem } from "../src/index.js";

it("executes all six catalog views against current, historical, and structured telemetry rows", { timeout: 60_000 }, async () => {
  const endpoint = process.env["SCOPEDB_TEST_ENDPOINT"];
  assert.ok(endpoint, "Set SCOPEDB_TEST_ENDPOINT to a local ScopeDB test server");
  const client = new Client(endpoint, { headers: { "X-ScopeDB-Tenant-Id": "__system__" } });
  const database = "tail_test_" + randomUUID().replaceAll("-", "");
  const table = YAML.parse(await readFile(new URL("../../catalog/table.yaml", import.meta.url), "utf8")) as CatalogTable;
  const views: CatalogView[] = [];
  for (const name of ["requests", "pageviews", "errors", "ai_bots", "rum", "traces"]) {
    const view = YAML.parse(await readFile(new URL("../../catalog/v_cloudflare_" + name + ".yaml", import.meta.url), "utf8")) as CatalogView;
    view.statement = view.statement.replaceAll("scopedb.public.", database + ".public.");
    views.push(view);
  }
  await client.catalog().database(database).apply({ name: database, schemas: [{ name: "public", tables: [table], views }] });
  try {
    const navigation = { "user-agent": "Mozilla/5.0", "sec-fetch-mode": "navigate", "sec-fetch-dest": "document" };
    function request(service: string, path: string, headers: Record<string, string> = navigation, cf = {}, status = 200): TailItem {
      return {
        scriptName: service,
        outcome: "ok",
        eventTimestamp: 1730620800000,
        event: { request: { url: "https://example.com" + path, method: "GET", headers, cf }, response: { status } },
      };
    }
    const rows = [
      request("document", "/docs?utm_source=first&utm_source=last&tag=a&tag=b"),
      request("fetch", "/health", { ...navigation, "sec-fetch-mode": "cors", "sec-fetch-dest": "" }),
      request("rsc", "/docs", { ...navigation, rsc: "1" }),
      request("prefetch", "/docs", { ...navigation, purpose: "prefetch" }),
      request("no-navigation-headers", "/docs", { "user-agent": "Mozilla/5.0" }),
      request("search", "/search", navigation, { verifiedBotCategory: "Search Engine Crawler" }),
      request("monitor", "/health", navigation, { verifiedBotCategory: "Monitoring & Analytics" }),
      request("verified-ai", "/docs", navigation, { verifiedBotCategory: "AI Crawler" }),
      request("claimed-ai", "/docs", { ...navigation, "user-agent": "ChatGPT-User/1.0" }),
      request("markdown", "/docs.md"),
      { scriptName: "scheduled", outcome: "ok" },
      { scriptName: "client-canceled", outcome: "canceled" },
      { ...request("not-found-probe", "/.env", {}, {}, 404), logs: [{ level: "error", message: ["[404] GET /.env"] }] },
      { ...request("gateway-502", "/api/query", {}, {}, 502), logs: [{ level: "warn", message: ["cancel timeout"] }] },
      {
        scriptName: "uncaught-exc",
        outcome: "exception",
        exceptions: [{ name: "TypeError", message: "boom" }],
      },
      { ...request("logged-error", "/health", {}), logs: [{ level: "error", message: ["upstream failed"] }] },
      {
        scriptName: "telemetry-producer",
        outcome: "ok",
        event: {
          request: {
            url: "https://example.com/api/telemetry",
            method: "POST",
            headers: { traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" },
            cf: { country: "US", colo: "SJC" },
          },
          response: { status: 204 },
        },
        logs: [
          {
            level: "info",
            message: [
              {
                __scopedb_telemetry: 1,
                environment: "production",
                app_version: "1.0.0",
                events: [
                  {
                    event_type: "rum.error",
                    path: "/docs?utm_source=hn",
                    attributes: { session_id: "sess-1", error_type: "ReferenceError" },
                  },
                  {
                    event_type: "trace.span",
                    span_id: "00f067aa0ba902b7",
                    duration_ms: 21.5,
                    attributes: {
                      span_name: "POST /api/query",
                      span_kind: "SERVER",
                      status_code: "ERROR",
                      "http.request.method": "POST",
                      "http.route": "/api/query",
                      "http.response.status_code": 502,
                      "error.type": "http_502",
                      "scopedb.failure.stage": "upstream_transport",
                    },
                  },
                ],
              },
            ],
          },
        ],
      },
    ].flatMap((item) => tailItemToRows(item));
    const [legacy] = tailItemToRows(request("legacy", "/docs"));
    delete legacy!.attributes["query"];
    legacy!.attributes["utm_source"] = "legacy-campaign";
    rows.push(legacy!);
    await client.table(table.name, { database }).append(rows.map((row) => JSON.stringify(row)).join("\n"));
    async function query(statement: string) {
      return (await client.query(statement, { signal: AbortSignal.timeout(15_000) })).toObjects();
    }
    const source = database + ".public.";
    assert.deepEqual(await query("FROM " + source + "v_cloudflare_requests WHERE service = 'document' SELECT utm_source, query['tag'][1]::string AS second_tag"), [
      { utm_source: "first", second_tag: "b" },
    ]);
    assert.deepEqual(await query("FROM " + source + "v_cloudflare_requests WHERE service = 'legacy' SELECT utm_source"), [
      { utm_source: "legacy-campaign" },
    ]);
    assert.deepEqual(await query("FROM " + source + "v_cloudflare_pageviews SELECT service ORDER BY service"), [
      { service: "document" }, { service: "legacy" },
    ]);
    assert.deepEqual(await query("FROM " + source + "v_cloudflare_ai_bots SELECT service, is_verified_ai_bot, is_ai_user_agent, is_markdown_request ORDER BY service"), [
      { service: "claimed-ai", is_verified_ai_bot: false, is_ai_user_agent: true, is_markdown_request: false },
      { service: "markdown", is_verified_ai_bot: false, is_ai_user_agent: false, is_markdown_request: true },
      { service: "verified-ai", is_verified_ai_bot: true, is_ai_user_agent: false, is_markdown_request: false },
    ]);
    assert.deepEqual(await query("FROM " + source + "v_cloudflare_errors SELECT service, event_type ORDER BY service"), [
      { service: "gateway-502", event_type: "worker.invocation" },
      { service: "logged-error", event_type: "worker.log" },
      { service: "uncaught-exc", event_type: "worker.exception" },
    ]);
    assert.deepEqual(await query("FROM " + source + "v_cloudflare_rum SELECT service, event_type, utm_source, country, error_type"), [
      { service: "telemetry-producer", event_type: "rum.error", utm_source: "hn", country: "US", error_type: "ReferenceError" },
    ]);
    assert.deepEqual(await query("FROM " + source + "v_cloudflare_traces SELECT service, method, http_route, status, error_type, failure_stage"), [
      { service: "telemetry-producer", method: "POST", http_route: "/api/query", status: 502, error_type: "http_502", failure_stage: "upstream_transport" },
    ]);
  } finally {
    await client.catalog().database(database).delete({ allowDestructive: true });
  }
});
