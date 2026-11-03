# Cloudflare Tail Worker for ScopeDB

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/scopedb/cloudflare-tail-worker)

Stream Cloudflare Worker invocations, `console.*` logs, and uncaught exceptions directly into [ScopeDB](https://www.scopedb.io) without modifying your application Workers.

## How it works

In Cloudflare Workers, a single [Tail Worker](https://developers.cloudflare.com/workers/observability/logs/tail-workers/) (`scopedb-tail`) can receive execution events from any number of producer Workers in the same Cloudflare account via `tail_consumers`. Every event batch delivered by Cloudflare includes the producer Worker's `scriptName`, which `scopedb-tail` maps to the `service` column in ScopeDB:

```text
┌───────────────────┐  tail_consumers (scriptName = "site-worker")
│    site-worker    ├─────────────────────────────────────────────┐
└───────────────────┘                                             ▼
┌───────────────────┐  tail_consumers (scriptName = "api-worker")  ┌──────────────────┐  HTTPS (NDJSON + gzip)  ┌──────────────────────────┐
│    api-worker     ├────────────────────────────────────────────►│   scopedb-tail   ├────────────────────────►│       ScopeDB Cloud      │
└───────────────────┘                                             │ (1 per account)  │                         │ cloudflare_worker_events │
┌───────────────────┐  tail_consumers (scriptName = "app-console") └──────────────────┘                         └──────────────────────────┘
│    app-console    ├─────────────────────────────────────────────▲
└───────────────────┘
```

- **Zero application code changes**: Producer Workers only add `tail_consumers = [{ service = "scopedb-tail" }]` to `wrangler.toml`.
- **Zero Node.js compatibility polyfills**: Built on the [`scopedb`](https://github.com/scopedb/scopedb-js) Web Fetch + `CompressionStream` runtime (`< 10 KiB`, no `nodejs_compat` required).
- **Safe by default**: URL query strings, URL fragments, `Referer` paths/queries, and sensitive headers (`Authorization`, `Cookie`, `Set-Cookie`) are never stored. Arbitrary `console.log` payloads (`BigInt`, `NaN`/`Infinity`, `Error`, `Date`, circular references) are sanitized before serialization.
- **Cost-efficient on Cloudflare**: Cloudflare Tail Worker invocations do not incur per-request charges and are billed only on Worker CPU milliseconds, bypassing Cloudflare Observability per-GB ingestion and storage meters.

## 3-step quickstart

### 1. Create the table and views in ScopeDB

Run [schema.scopeql](schema.scopeql) in the ScopeDB Console SQL editor or with the [ScopeDB CLI](https://github.com/scopedb/scopedb-cli) (`scope query -f schema.scopeql`). This creates the base table and three ready-to-query views:

| Object                                     | Kind  | Purpose                                                                                                             |
| ------------------------------------------ | ----- | ------------------------------------------------------------------------------------------------------------------- |
| `scopedb.public.cloudflare_worker_events`  | Table | Base event table storing `worker.invocation`, `worker.log`, and `worker.exception` rows clustered by `service`      |
| `scopedb.public.v_cloudflare_requests`     | View  | Flattened HTTP/Worker invocation metrics (`wall_time_ms`, `cpu_time_ms`, `colo`, `country`, `user_agent`, `status`) |
| `scopedb.public.v_cloudflare_errors`       | View  | All `5xx` responses, non-`ok` Worker outcomes, `console.error` logs, and uncaught exceptions with stack traces      |
| `scopedb.public.v_cloudflare_ai_bots`      | View  | Requests to `/llms.txt` or `*.md` and traffic from AI crawlers and coding agents (`ClaudeBot`, `GPTBot`, `Cursor`)  |

### 2. Deploy `scopedb-tail` once in your Cloudflare account

Click **Deploy to Cloudflare** above, or clone this repository and deploy with Wrangler:

```bash
git clone https://github.com/scopedb/cloudflare-tail-worker.git
cd cloudflare-tail-worker
pnpm install
pnpm exec wrangler secret put SCOPEDB_ENDPOINT
pnpm exec wrangler secret put SCOPEDB_API_KEY
pnpm run deploy
```

| Binding            | Type   | Default                    | Description                                                                   |
| ------------------ | ------ | -------------------------- | ----------------------------------------------------------------------------- |
| `SCOPEDB_ENDPOINT` | Secret | *(Required)*               | ScopeDB Cloud workspace endpoint (for example, `https://<id>.scopedb.cloud`)  |
| `SCOPEDB_API_KEY`  | Secret | *(Required)*               | ScopeDB workspace API key with append access to the target table              |
| `SCOPEDB_DATABASE` | Var    | `scopedb`                  | Target database name in [wrangler.toml](wrangler.toml)                        |
| `SCOPEDB_SCHEMA`   | Var    | `public`                   | Target schema name in [wrangler.toml](wrangler.toml)                          |
| `SCOPEDB_TABLE`    | Var    | `cloudflare_worker_events` | Target table name in [wrangler.toml](wrangler.toml)                           |

### 3. Attach your producer Workers

Add `tail_consumers` to the `wrangler.toml` of each Worker in your Cloudflare account that you want to observe, then redeploy those Workers:

```toml
tail_consumers = [{ service = "scopedb-tail" }]
```

If a producer Worker defines named environments (such as `[env.staging]` and `[env.production]`), add `tail_consumers = [{ service = "scopedb-tail" }]` under each environment block in `wrangler.toml`.

## Example ScopeQL queries

### Compare request volume, errors, and CPU time by Worker service

```scopeql
FROM scopedb.public.v_cloudflare_requests
WHERE occurred_at >= NOW() - '24h'::interval
GROUP BY service
AGGREGATE
    count() AS requests,
    sum(if(status >= 500 OR outcome != 'ok', 1, 0)) AS errors,
    avg(wall_time_ms) AS avg_wall_ms,
    max(cpu_time_ms) AS max_cpu_ms
ORDER BY requests DESC
```

### Inspect recent 5xx failures and uncaught exceptions

```scopeql
FROM scopedb.public.v_cloudflare_errors
WHERE occurred_at >= NOW() - '6h'::interval
SELECT occurred_at, service, event_type, outcome, status, path, message, ray_id, stack
ORDER BY occurred_at DESC
LIMIT 50
```

### See which AI agents and crawlers read `/llms.txt` and Markdown docs

```scopeql
FROM scopedb.public.v_cloudflare_ai_bots
WHERE occurred_at >= NOW() - '7d'::interval
GROUP BY path, user_agent, as_organization
AGGREGATE count() AS fetches
ORDER BY fetches DESC
LIMIT 25
```

### Correlate all logs and exceptions from a single invocation

```scopeql
FROM scopedb.public.cloudflare_worker_events
WHERE attributes['invocation_id']::string = '<invocation_id>'
SELECT occurred_at, event_type, level, message, attributes
ORDER BY occurred_at ASC
```

## Custom filtering and sampling

[src/index.ts](src/index.ts) exports `createTailWorker(options)` so you can filter health checks or sample successful `2xx` invocations while keeping `100%` of errors and exceptions:

```ts
import { createTailWorker } from "./src/index.js";

export default createTailWorker({
  shouldRecord: (item) => {
    const status = item.event?.response?.status ?? 0;
    if (item.outcome !== "ok" || status >= 400 || (item.exceptions?.length ?? 0) > 0) {
      return true;
    }
    return Math.random() < 0.25;
  },
});
```

## Development

```bash
pnpm install
pnpm run ci
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
