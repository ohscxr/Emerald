import { DurableObject } from "cloudflare:workers";

// Routes /ws to the single BarRoom Durable Object; everything else is the static site in /public.
export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/ws") {
      if (req.headers.get("Upgrade") !== "websocket") return new Response("Expected WebSocket", { status: 426 });
   const ALLOWED_ORIGINS = ["https://option2.live", "https://www.option2.live"];
   const origin = req.headers.get("Origin");
   if (origin && new URL(origin).host !== url.host && !ALLOWED_ORIGINS.includes(origin)) return new Response("Forbidden", { status: 403 });    }
    return env.ASSETS.fetch(req);
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
