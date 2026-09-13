// GO pub — order board backend.
// One Durable Object instance ("main") holds:
//  - orders: active kitchen/bar tickets (accept -> ready -> served)
//  - openTables: tables currently occupied (a "tab"), independent of
//    individual ticket status — stays open across multiple rounds until
//    the waiter explicitly closes the table
//  - history: served order tickets (used to reconstruct a table's full bill)
//  - closedTables: finalized receipts for closed tables
// Everything is broadcast to every connected client (waiter / cook /
// bartender / manager) over WebSocket. Each role must present the
// matching PIN (set in wrangler.toml [vars]) before it can send anything.

const HISTORY_LIMIT = 500;
const CLOSED_TABLES_LIMIT = 300;
const SERVICE_RATE = 0.10;

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
    this.openTables = {};
    this.closedTables = [];
    this.staff = [];       // [{id, role, name}]
    this.tableCount = 20;
    this.unavailable = {}; // key "kitchen|Name" or "bar|Name" -> {comment, disabledBy, disabledAt}
    this.pins = { waiter: "1111", cook: "1111", bartender: "1111", manager: "1111" };
    this.ready = this.state.blockConcurrencyWhile(async () => {
      const storedOrders = await this.state.storage.get("orders");
      const storedHistory = await this.state.storage.get("history");
      const storedOpen = await this.state.storage.get("openTables");
      const storedClosed = await this.state.storage.get("closedTables");
      const storedStaff = await this.state.storage.get("staff");
      const storedTableCount = await this.state.storage.get("tableCount");
      const storedUnavailable = await this.state.storage.get("unavailable");
      const storedPins = await this.state.storage.get("pins");
      if (storedOrders) this.orders = storedOrders;
      if (storedHistory) this.history = storedHistory;
      if (storedOpen) this.openTables = storedOpen;
      if (storedClosed) this.closedTables = storedClosed;
      if (storedStaff) this.staff = storedStaff;
      if (storedTableCount) this.tableCount = storedTableCount;
      if (storedUnavailable) this.unavailable = storedUnavailable;
      if (storedPins) this.pins = storedPins;
    });
  }

  async persist() {
    await this.state.storage.put("orders", this.orders);
    await this.state.storage.put("history", this.history);
    await this.state.storage.put("openTables", this.openTables);
    await this.state.storage.put("closedTables", this.closedTables);
    await this.state.storage.put("staff", this.staff);
    await this.state.storage.put("tableCount", this.tableCount);
    await this.state.storage.put("unavailable", this.unavailable);
    await this.state.storage.put("pins", this.pins);
  }

  stateSnapshot() {
    return {
      type: "state",
      orders: this.orders,
      history: this.history,
      openTables: this.openTables,
      closedTables: this.closedTables,
      staff: this.staff,
      tableCount: this.tableCount,
      unavailable: this.unavailable,
    };
  }

  broadcast() {
    const payload = JSON.stringify(this.stateSnapshot());
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

  sendTo(conn, message) {
    try { conn.ws.send(JSON.stringify(message)); } catch (e) {}
  }

  checkPin(role, pin) {
    const expected = this.pins[role];
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
          if (msg.staffId) {
            const staff = this.staff.find(s => s.id === msg.staffId && s.role === msg.role);
            if (!staff) {
              server.send(JSON.stringify({ type: "auth_error", reason: "unknown_staff" }));
              server.close(4001, "unknown staff");
              this.sockets.delete(conn);
              return;
            }
            conn.role = msg.role;
            conn.staffId = staff.id;
            conn.staffName = staff.name || "";
            conn.authed = true;
          } else {
            if (!this.checkPin(msg.role, msg.pin)) {
              server.send(JSON.stringify({ type: "auth_error" }));
              server.close(4001, "bad pin");
              this.sockets.delete(conn);
              return;
            }
            conn.role = msg.role;
            conn.authed = true;
          }
          server.send(JSON.stringify({ type: "auth_ok" }));
          server.send(JSON.stringify(this.stateSnapshot()));
          return;
        }

        if (msg.type === "get_receipt") {
          // Public, unauthenticated lookup — this is what a QR code or a
          // shared WhatsApp/Telegram link points a guest or a manager to.
          const receipt = this.closedTables.find(r => r.id === msg.id);
          this.sendTo(conn, receipt
            ? { type: "receipt_data", receipt }
            : { type: "receipt_not_found", id: msg.id });
          return;
        }

        if (!conn.authed) return; // ignore everything until hello succeeds

        if (msg.type === "set_staff_name") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff && conn.staffId === msg.staffId) {
            staff.name = String(msg.name || "").slice(0, 40);
            conn.staffName = staff.name;
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "set_display_name" && !conn.staffId) {
          // For connections logged in with the shared role PIN (no personal
          // staff record) — the name only lives on this connection and is
          // re-sent by the client after every reconnect.
          conn.staffName = String(msg.name || "").slice(0, 40);
        }

        if (msg.type === "change_pin" && conn.role === msg.role) {
          const newPin = String(msg.newPin || "").trim();
          if (newPin.length >= 4 && newPin.length <= 6 && /^\d+$/.test(newPin)) {
            this.pins[msg.role] = newPin;
            await this.persist();
            this.sendTo(conn, { type: "pin_changed", role: msg.role });
          } else {
            this.sendTo(conn, { type: "pin_change_error", reason: "invalid" });
          }
        }

        if (msg.type === "create_staff" && conn.role === "manager") {
          const staff = { id: crypto.randomUUID(), role: msg.role, name: "" };
          this.staff.push(staff);
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "delete_staff" && conn.role === "manager") {
          this.staff = this.staff.filter(s => s.id !== msg.staffId);
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "reassign_staff" && conn.role === "manager") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff) {
            staff.id = crypto.randomUUID(); // old device's cached id stops matching anyone
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "set_table_count" && conn.role === "manager") {
          const n = parseInt(msg.count, 10);
          if (n && n > 0 && n <= 200) {
            this.tableCount = n;
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "disable_items" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          (msg.items || []).forEach(i => {
            this.unavailable[dest + "|" + i.name] = {
              comment: String(i.comment || "").slice(0, 200),
              disabledBy: conn.staffName || null,
              disabledAt: Date.now(),
            };
          });
          await this.persist();
          this.broadcast();
          this.notify("manager", { kind: "menu_disabled", dest, names: (msg.items || []).map(i => i.name) });
        }

        if (msg.type === "enable_items" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          (msg.names || []).forEach(name => { delete this.unavailable[dest + "|" + name]; });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "new_order") {
          const table = msg.table;
          if (!this.openTables[table]) {
            this.openTables[table] = { openedAt: Date.now() };
          }

          const isAvailable = (dest, name) => !this.unavailable[dest + "|" + name];
          const rawKitchen = msg.kitchenItems || [];
          const rawBar = msg.barItems || [];
          const kitchenItems = rawKitchen.filter(i => isAvailable("kitchen", i.name));
          const barItems = rawBar.filter(i => isAvailable("bar", i.name));
          const removed = [
            ...rawKitchen.filter(i => !isAvailable("kitchen", i.name)).map(i => i.name),
            ...rawBar.filter(i => !isAvailable("bar", i.name)).map(i => i.name),
          ];

          const order = {
            id: crypto.randomUUID(),
            table,
            createdAt: Date.now(),
            waiterName: conn.staffName || "",
            kitchenItems,
            barItems,
            kitchenStatus: kitchenItems.length ? "pending" : "none",
            barStatus: barItems.length ? "pending" : "none",
            kitchenAcceptedAt: null,
            kitchenReadyAt: null,
            barAcceptedAt: null,
            barReadyAt: null,
            cookName: null,
            bartenderName: null,
          };
          this.orders.push(order);
          await this.persist();
          this.broadcast();
          if (order.kitchenStatus === "pending") this.notify("cook", { table: order.table, orderId: order.id });
          if (order.barStatus === "pending") this.notify("bartender", { table: order.table, orderId: order.id });
          if (removed.length) this.sendTo(conn, { type: "items_removed", names: removed });
        }

        if (msg.type === "kitchen_accept") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order && order.kitchenStatus === "pending") {
            order.kitchenStatus = "accepted";
            order.kitchenAcceptedAt = Date.now();
            order.cookName = conn.staffName || "";
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "bar_accept") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order && order.barStatus === "pending") {
            order.barStatus = "accepted";
            order.barAcceptedAt = Date.now();
            order.bartenderName = conn.staffName || "";
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

        if (msg.type === "close_table") {
          const table = msg.table;
          const session = this.openTables[table];
          if (!session) {
            this.sendTo(conn, { type: "close_error", table, reason: "not_open" });
            return;
          }
          const pending = this.orders.filter(o => o.table === table);
          if (pending.length > 0) {
            this.sendTo(conn, { type: "close_error", table, reason: "pending_items" });
            return;
          }

          const rounds = this.history.filter(h => h.table === table && h.servedAt >= session.openedAt);
          const merged = {}; // key: dest|name -> {name, qty, price}
          rounds.forEach(r => {
            (r.kitchenItems || []).forEach(i => {
              const key = "kitchen|" + i.name;
              if (!merged[key]) merged[key] = { name: i.name, qty: 0, price: i.price };
              merged[key].qty += i.qty;
            });
            (r.barItems || []).forEach(i => {
              const key = "bar|" + i.name;
              if (!merged[key]) merged[key] = { name: i.name, qty: 0, price: i.price };
              merged[key].qty += i.qty;
            });
          });
          const items = Object.values(merged);
          const subtotal = items.reduce((s, i) => s + parsePrice(i.price) * i.qty, 0);
          const service = Math.round(subtotal * SERVICE_RATE);
          const total = subtotal + service;

          const receipt = {
            id: crypto.randomUUID(),
            table,
            openedAt: session.openedAt,
            closedAt: Date.now(),
            closedBy: conn.staffName || "",
            items,
            subtotal,
            service,
            total,
          };

          delete this.openTables[table];
          this.closedTables.push(receipt);
          if (this.closedTables.length > CLOSED_TABLES_LIMIT) this.closedTables = this.closedTables.slice(-CLOSED_TABLES_LIMIT);

          await this.persist();
          this.broadcast();
          this.sendTo(conn, { type: "table_closed", receipt });
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
