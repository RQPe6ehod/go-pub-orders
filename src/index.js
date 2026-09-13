// GO pub — order board backend.
// One Durable Object instance ("main") holds all active orders plus a
// short served-order history, and broadcasts every change to every
// connected client (waiter / cook / bartender / manager) over WebSocket.
// Each role must present the matching PIN (set in wrangler.toml [vars])
// before it is allowed to send anything.

const HISTORY_LIMIT = 500;

function parsePrice(p) {
  if (typeof p === "number") return p;
  if (!p) return 0;
  const n = parseInt(String(p).replace(/[^\d]/g, ""), 10);
  return isNaN(n) ? 0 : n;
}

function lineTotal(items) {
  return (items || []).reduce((sum, i) => sum + parsePrice(i.price) * (i.qty || 1), 0);
}

export class OrderBoard {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.sockets = new Set(); // { ws, role, authed }
    this.orders = [];
    this.history = [];
    this.ready = this.state.blockConcurrencyWhile(async () => {
      const storedOrders = await this.state.storage.get("orders");
      const storedHistory = await this.state.storage.get("history");
      if (storedOrders) this.orders = storedOrders;
      if (storedHistory) this.history = storedHistory;
    });
  }

  async persist() {
    await this.state.storage.put("orders", this.orders);
    await this.state.storage.put("history", this.history);
  }

  broadcast() {
    const payload = JSON.stringify({ type: "state", orders: this.orders, history: this.history });
    for (const client of this.sockets) {
      try { client.ws.send(payload); } catch (e) { /* ignore dead sockets */ }
    }
  }

  notify(role, message) {
    const payload = JSON.stringify({ type: "notify", ...message });
    for (const client of this.sockets) {
      if (client.role === role) {
        try { client.ws.send(payload); } catch (e) {}
      }
    }
  }

  checkPin(role, pin) {
    const expected = this.env["PIN_" + String(role || "").toUpperCase()];
    if (!expected) return true; // no PIN configured for this role -> allow
    return String(pin || "") === String(expected);
  }

  async fetch(request) {
    await this.ready;
    const url = new URL(request.url);

    if (url.pathname === "/ws") {
      const upgrade = request.headers.get("Upgrade");
      if (upgrade !== "websocket") {
        return new Response("Expected WebSocket", { status: 426 });
      }
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);
      server.accept();

      const conn = { ws: server, role: null, authed: false };
      this.sockets.add(conn);

      server.addEventListener("message", async (evt) => {
        let msg;
        try { msg = JSON.parse(evt.data); } catch (e) { return; }

        if (msg.type === "hello") {
          if (!this.checkPin(msg.role, msg.pin)) {
            server.send(JSON.stringify({ type: "auth_error" }));
            server.close(4001, "bad pin");
            this.sockets.delete(conn);
            return;
          }
          conn.role = msg.role;
          conn.authed = true;
          server.send(JSON.stringify({ type: "auth_ok" }));
          server.send(JSON.stringify({ type: "state", orders: this.orders, history: this.history }));
          return;
        }

        if (!conn.authed) return; // ignore everything until hello+pin succeeds

        if (msg.type === "new_order") {
          const order = {
            id: crypto.randomUUID(),
            table: msg.table,
            createdAt: Date.now(),
            kitchenItems: msg.kitchenItems || [],
            barItems: msg.barItems || [],
            kitchenStatus: (msg.kitchenItems && msg.kitchenItems.length) ? "pending" : "none",
            barStatus: (msg.barItems && msg.barItems.length) ? "pending" : "none",
            kitchenAcceptedAt: null,
            kitchenReadyAt: null,
            barAcceptedAt: null,
            barReadyAt: null,
          };
          this.orders.push(order);
          await this.persist();
          this.broadcast();
          if (order.kitchenStatus === "pending") this.notify("cook", { table: order.table, orderId: order.id });
          if (order.barStatus === "pending") this.notify("bartender", { table: order.table, orderId: order.id });
        }

        if (msg.type === "kitchen_accept") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order && order.kitchenStatus === "pending") {
            order.kitchenStatus = "accepted";
            order.kitchenAcceptedAt = Date.now();
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "bar_accept") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order && order.barStatus === "pending") {
            order.barStatus = "accepted";
            order.barAcceptedAt = Date.now();
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "kitchen_ready") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order) {
            order.kitchenStatus = "ready";
            order.kitchenReadyAt = Date.now();
            await this.persist();
            this.broadcast();
            this.notify("waiter", { table: order.table, orderId: order.id, part: "kitchen" });
          }
        }

        if (msg.type === "bar_ready") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order) {
            order.barStatus = "ready";
            order.barReadyAt = Date.now();
            await this.persist();
            this.broadcast();
            this.notify("waiter", { table: order.table, orderId: order.id, part: "bar" });
          }
        }

        if (msg.type === "cancel_order") {
          const order = this.orders.find(o => o.id === msg.orderId);
          this.orders = this.orders.filter(o => o.id !== msg.orderId);
          await this.persist();
          this.broadcast();
          if (order) {
            if (order.kitchenItems.length) this.notify("cook", { table: order.table, orderId: order.id, cancelled: true });
            if (order.barItems.length) this.notify("bartender", { table: order.table, orderId: order.id, cancelled: true });
          }
        }

        if (msg.type === "served") {
          const order = this.orders.find(o => o.id === msg.orderId);
          this.orders = this.orders.filter(o => o.id !== msg.orderId);
          if (order) {
            order.servedAt = Date.now();
            order.total = lineTotal(order.kitchenItems) + lineTotal(order.barItems);
            this.history.push(order);
            if (this.history.length > HISTORY_LIMIT) this.history = this.history.slice(-HISTORY_LIMIT);
          }
          await this.persist();
          this.broadcast();
        }
      });

      server.addEventListener("close", () => this.sockets.delete(conn));
      server.addEventListener("error", () => this.sockets.delete(conn));

      return new Response(null, { status: 101, webSocket: client });
    }

    return new Response("GO pub order board", { status: 200 });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const cors = {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "*",
    };
    if (request.method === "OPTIONS") return new Response(null, { headers: cors });

    if (url.pathname === "/ws") {
      const id = env.ORDER_BOARD.idFromName("main");
      const stub = env.ORDER_BOARD.get(id);
      return stub.fetch(request);
    }

    return new Response("GO pub order board is running.", { headers: cors });
  },
};

