import fs from "node:fs";
import { createSqlJsAdapter } from "./sqljsAdapter.js";

function getHttpUrl(url) {
  if (!url) return null;
  return url.replace(/^libsql:\/\//, "https://");
}

export async function createTursoAdapter(filePath, databaseUrl, authToken) {
  const httpUrl = getHttpUrl(databaseUrl);
  const pipelineUrl = `${httpUrl.replace(/\/+$/, "")}/v2/pipeline`;

  // 1. Download existing snapshot from Turso on boot if exists
  if (httpUrl && authToken) {
    try {
      console.log(`[Turso] Checking remote snapshot at ${httpUrl}...`);
      const res = await fetch(pipelineUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${authToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          requests: [
            {
              type: "execute",
              stmt: {
                sql: "SELECT value FROM kv WHERE scope = '_turso_snapshot' AND key = 'data' LIMIT 1",
              },
            },
            { type: "close" },
          ],
        }),
      });

      if (res.ok) {
        const json = await res.json();
        const row = json.results?.[0]?.response?.result?.rows?.[0];
        if (row && row[0]?.value) {
          const b64 = row[0].value;
          const buf = Buffer.from(b64, "base64");
          fs.writeFileSync(filePath, buf);
          console.log(`[Turso] Successfully pulled database snapshot (${buf.length} bytes) from Turso!`);
        } else {
          console.log("[Turso] Remote snapshot empty. Starting with fresh/local database.");
        }
      }
    } catch (err) {
      console.warn(`[Turso] Warning: failed to fetch remote snapshot: ${err.message}`);
    }
  }

  // 2. Initialize in-memory / fast local SQLite via sql.js
  const baseAdapter = await createSqlJsAdapter(filePath);

  let dirty = false;
  let pushTimer = null;
  const PUSH_DEBOUNCE_MS = 2000; // debounce Turso upload 2s

  async function pushToTurso() {
    if (!httpUrl || !authToken) return;
    try {
      if (!fs.existsSync(filePath)) return;
      const buf = fs.readFileSync(filePath);
      const b64 = buf.toString("base64");

      const res = await fetch(pipelineUrl, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${authToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          requests: [
            {
              type: "execute",
              stmt: {
                sql: "CREATE TABLE IF NOT EXISTS kv (scope TEXT NOT NULL, key TEXT NOT NULL, value TEXT, PRIMARY KEY (scope, key))",
              },
            },
            {
              type: "execute",
              stmt: {
                sql: "INSERT OR REPLACE INTO kv (scope, key, value) VALUES ('_turso_snapshot', 'data', ?)",
                args: [{ type: "text", value: b64 }],
              },
            },
            { type: "close" },
          ],
        }),
      });

      if (res.ok) {
        dirty = false;
        console.log(`[Turso] Successfully synchronized snapshot (${buf.length} bytes) to Turso.`);
      } else {
        const text = await res.text();
        console.warn(`[Turso] Sync failed HTTP ${res.status}: ${text}`);
      }
    } catch (e) {
      console.warn(`[Turso] Sync network error: ${e.message}`);
    }
  }

  function scheduleTursoPush() {
    dirty = true;
    if (pushTimer) clearTimeout(pushTimer);
    pushTimer = setTimeout(() => {
      pushTimer = null;
      if (dirty) pushToTurso().catch(() => {});
    }, PUSH_DEBOUNCE_MS);
  }

  // Intercept write operations
  function run(sql, params = []) {
    const res = baseAdapter.run(sql, params);
    scheduleTursoPush();
    return res;
  }

  function exec(sql) {
    baseAdapter.exec(sql);
    scheduleTursoPush();
  }

  function transaction(fn) {
    const res = baseAdapter.transaction(fn);
    scheduleTursoPush();
    return res;
  }

  function close() {
    if (pushTimer) clearTimeout(pushTimer);
    if (dirty) pushToTurso().catch(() => {});
    baseAdapter.close();
  }

  const flush = () => {
    if (dirty) pushToTurso().catch(() => {});
  };
  process.on("beforeExit", flush);
  process.on("SIGINT", flush);
  process.on("SIGTERM", flush);

  return {
    ...baseAdapter,
    driver: "turso-sqljs",
    run,
    exec,
    transaction,
    close,
  };
}
