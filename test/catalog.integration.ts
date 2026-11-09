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

it("executes catalog views against current and historical event rows", { timeout: 60_000 }, async () => {
  const endpoint = process.env["SCOPEDB_TEST_ENDPOINT"];
  assert.ok(endpoint, "Set SCOPEDB_TEST_ENDPOINT to a local ScopeDB test server");
  const client = new Client(endpoint, { headers: { "X-ScopeDB-Tenant-Id": "__system__" } });
  const database = "tail_test_" + randomUUID().replaceAll("-", "");
  const table = YAML.parse(await readFile(new URL("../../catalog/table.yaml", import.meta.url), "utf8")) as CatalogTable;
  const views: CatalogView[] = [];
  for (const name of ["requests", "pageviews", "errors", "ai_bots"]) {
    const view = YAML.parse(await readFile(new URL("../../catalog/v_cloudflare_" + name + ".yaml", import.meta.url), "utf8")) as CatalogView;
    view.statement = view.statement.replaceAll("scopedb.public.", database + ".public.");
    views.push(view);
  }
  await client.catalog().database(database).apply({ name: database, schemas: [{ name: "public", tables: [table], views }] });
  try {
    const navigation = { "user-agent": "Mozilla/5.0", "sec-fetch-mode": "navigate", "sec-fetch-dest": "document" };
    function request(service: string, path: string, headers: Record<string, string> = navigation, cf = {}): TailItem {
      return { scriptName: service, outcome: "ok", eventTimestamp: 1730620800000,
        event: { request: { url: "https://example.com" + path, method: "GET", headers, cf }, response: { status: 200 } },
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
      { ...request("logged-error", "/health", {}), logs: [{ level: "error", message: ["upstream failed"] }] },
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
    assert.deepEqual(await query("FROM " + source + "v_cloudflare_errors SELECT service, event_type"), [
      { service: "logged-error", event_type: "worker.log" },
    ]);
  } finally {
    await client.catalog().database(database).delete({ allowDestructive: true });
  }
});
