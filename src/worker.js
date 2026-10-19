import { DurableObject } from "cloudflare:workers";
import page from "./index.html"; // Wrangler bundles .html files as text, so no /public folder is needed.

// Sites allowed to open the WebSocket besides the Worker's own address.
// Add more with a comma-separated ALLOWED_ORIGINS variable, or edit this list.
const DEFAULT_ORIGINS = ["https://option2.live", "https://www.option2.live"];

function originOk(origin, url, env) {
  if (!origin) return true; // non-browser clients; the staff passcode still applies
  let host;
  try { host = new URL(origin).host; } catch { return false; }
  if (host === url.host) return true;
  const extra = String(env.ALLOWED_ORIGINS || "").split(",").map((s) => s.trim()).filter(Boolean);
  return DEFAULT_ORIGINS.concat(extra).includes(origin);
}

// Routes /ws to the single BarRoom Durable Object and serves the page (index.html) at /.
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ws") {
      if (req.headers.get("Upgrade") !== "websocket") return new Response("Expected WebSocket", { status: 426 });
      if (!originOk(req.headers.get("Origin"), url, env)) return new Response("Forbidden", { status: 403 });
      return env.BAR.get(env.BAR.idFromName("main")).fetch(req);
    }
    if (req.method !== "GET" && req.method !== "HEAD") return new Response("Method not allowed", { status: 405 });
    if (url.pathname !== "/" && url.pathname !== "/index.html") return new Response("Not found", { status: 404 });
    return new Response(req.method === "HEAD" ? null : page, {
      headers: {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-cache",
        "x-content-type-options": "nosniff",
        "referrer-policy": "no-referrer",
      },
    });
  },
};

const PATH = /^([A-Za-z0-9_\-.~:@+]{1,200})\/([A-Za-z0-9_\-.~:@+]{1,200})$/;

function same(a, b) {
  if (a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return d === 0;
}

// One room holds every document (tables, entries, settings) in SQLite and pushes changes to all connected devices.
export class BarRoom extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec("CREATE TABLE IF NOT EXISTS docs(col TEXT NOT NULL, id TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(col, id))");
  }

  async fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ ok: false });
    return new Response(null, { status: 101, webSocket: client });
  }

  all() {
    return this.sql.exec("SELECT col, id, data FROM docs").toArray().map((r) => ({ col: r.col, id: r.id, data: JSON.parse(r.data) }));
  }

  broadcast(msg) {
    const text = JSON.stringify(msg);
    for (const ws of this.ctx.getWebSockets()) {
      const st = ws.deserializeAttachment();
      if (st && st.ok) { try { ws.send(text); } catch {} }
    }
  }

  webSocketMessage(ws, raw) {
    if (typeof raw !== "string" || raw.length > 100000) return;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    const reply = (o) => ws.send(JSON.stringify(o));

    if (m.op === "auth") {
      const need = this.env.STAFF_PIN;
      if (!need || same(String(m.pin || ""), String(need))) {
        ws.serializeAttachment({ ok: true });
        reply({ t: "authed", pin: !!need });
        reply({ t: "init", docs: this.all() });
      } else {
        reply({ t: "denied" });
      }
      return;
    }
    const st = ws.deserializeAttachment();
    if (!st || !st.ok) return reply({ t: "denied" });

    const rid = m.rid;
    const fail = (code) => reply({ t: "err", rid, code });
    const p = PATH.exec(String(m.path || ""));
    if (!p) return fail("invalid_argument");
    const [, col, id] = p;

    if (m.op === "delete") {
      this.sql.exec("DELETE FROM docs WHERE col = ? AND id = ?", col, id);
      this.broadcast({ t: "change", col, id, data: null });
      return reply({ t: "ack", rid });
    }
    if (m.op !== "set" && m.op !== "update") return fail("invalid_argument");
    if (!m.data || typeof m.data !== "object" || Array.isArray(m.data)) return fail("invalid_argument");

    let data = m.data;
    if (m.op === "update") {
      const row = this.sql.exec("SELECT data FROM docs WHERE col = ? AND id = ?", col, id).toArray()[0];
      if (!row) return fail("invalid_argument");
      data = Object.assign(JSON.parse(row.data), m.data);
    }
    const text = JSON.stringify(data);
    if (text.length > 50000) return fail("invalid_argument");
    this.sql.exec("INSERT OR REPLACE INTO docs(col, id, data) VALUES (?, ?, ?)", col, id, text);
    this.broadcast({ t: "change", col, id, data });
    reply({ t: "ack", rid });
  }

  webSocketClose(ws, code) {
    try { ws.close(code); } catch {}
  }
}
