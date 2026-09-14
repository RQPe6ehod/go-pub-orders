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

// ---------------------------------------------------------------------
// Web Push (RFC 8291 payload encryption + RFC 8292 VAPID), implemented
// with the platform's built-in Web Crypto API only — no npm dependency,
// since this Worker is deployed as a single plain file.
// ---------------------------------------------------------------------

function b64urlToBytes(s) {
  s = s.replace(/-/g, "+").replace(/_/g, "/");
  while (s.length % 4) s += "=";
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}
function bytesToB64url(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function concatBytes(...arrs) {
  const len = arrs.reduce((s, a) => s + a.length, 0);
  const out = new Uint8Array(len);
  let off = 0;
  for (const a of arrs) { out.set(a, off); off += a.length; }
  return out;
}
async function hmacSha256(keyBytes, dataBytes) {
  const key = await crypto.subtle.importKey("raw", keyBytes, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await crypto.subtle.sign("HMAC", key, dataBytes));
}
async function hkdfExpand(prk, info, length) {
  const t1 = await hmacSha256(prk, concatBytes(info, new Uint8Array([1])));
  return t1.slice(0, length);
}

async function encryptWebPushPayload(payloadBytes, p256dhB64url, authB64url) {
  const clientPublicKeyBytes = b64urlToBytes(p256dhB64url); // 65 bytes
  const authSecret = b64urlToBytes(authB64url); // 16 bytes

  const clientKey = await crypto.subtle.importKey("raw", clientPublicKeyBytes, { name: "ECDH", namedCurve: "P-256" }, false, []);
  const serverKeyPair = await crypto.subtle.generateKey({ name: "ECDH", namedCurve: "P-256" }, true, ["deriveBits"]);
  const serverPublicKeyBytes = new Uint8Array(await crypto.subtle.exportKey("raw", serverKeyPair.publicKey));

  const sharedSecret = new Uint8Array(await crypto.subtle.deriveBits({ name: "ECDH", public: clientKey }, serverKeyPair.privateKey, 256));

  const prkKey = await hmacSha256(authSecret, sharedSecret);
  const keyInfo = concatBytes(
    new TextEncoder().encode("WebPush: info"),
    new Uint8Array([0]),
    clientPublicKeyBytes,
    serverPublicKeyBytes
  );
  const ikm = await hkdfExpand(prkKey, keyInfo, 32);

  const salt = crypto.getRandomValues(new Uint8Array(16));
  const prk = await hmacSha256(salt, ikm);

  const cek = await hkdfExpand(prk, concatBytes(new TextEncoder().encode("Content-Encoding: aes128gcm"), new Uint8Array([0])), 16);
  const nonce = await hkdfExpand(prk, concatBytes(new TextEncoder().encode("Content-Encoding: nonce"), new Uint8Array([0])), 12);

  const paddedPlaintext = concatBytes(payloadBytes, new Uint8Array([2])); // delimiter for a single (final) record
  const cekKey = await crypto.subtle.importKey("raw", cek, { name: "AES-GCM" }, false, ["encrypt"]);
  const ciphertext = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce, tagLength: 128 }, cekKey, paddedPlaintext));

  const rsBytes = new Uint8Array(4);
  new DataView(rsBytes.buffer).setUint32(0, 4096, false);
  const header = concatBytes(salt, rsBytes, new Uint8Array([serverPublicKeyBytes.length]), serverPublicKeyBytes);

  return concatBytes(header, ciphertext);
}

async function buildVapidAuthHeader(endpoint, subject, publicKeyB64url, privateKeyPkcs8B64) {
  const aud = new URL(endpoint).origin;
  const header = { typ: "JWT", alg: "ES256" };
  const claims = { aud, exp: Math.floor(Date.now() / 1000) + 12 * 3600, sub: subject };
  const enc = (obj) => bytesToB64url(new TextEncoder().encode(JSON.stringify(obj)));
  const signingInput = enc(header) + "." + enc(claims);

  const pkcs8Bytes = Uint8Array.from(atob(privateKeyPkcs8B64), c => c.charCodeAt(0));
  const privateKey = await crypto.subtle.importKey("pkcs8", pkcs8Bytes, { name: "ECDSA", namedCurve: "P-256" }, false, ["sign"]);
  const sig = new Uint8Array(await crypto.subtle.sign({ name: "ECDSA", hash: "SHA-256" }, privateKey, new TextEncoder().encode(signingInput)));

  return `vapid t=${signingInput}.${bytesToB64url(sig)}, k=${publicKeyB64url}`;
}

async function sendWebPush(subscription, payloadObj, env) {
  const payloadBytes = new TextEncoder().encode(JSON.stringify(payloadObj));
  const body = await encryptWebPushPayload(payloadBytes, subscription.keys.p256dh, subscription.keys.auth);
  const auth = await buildVapidAuthHeader(
    subscription.endpoint,
    env.VAPID_SUBJECT || "mailto:admin@example.com",
    env.VAPID_PUBLIC_KEY,
    env.VAPID_PRIVATE_KEY_PKCS8
  );
  return fetch(subscription.endpoint, {
    method: "POST",
    headers: {
      Authorization: auth,
      "Content-Type": "application/octet-stream",
      "Content-Encoding": "aes128gcm",
      TTL: "60",
    },
    body,
  });
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
    this.rooms = [{ id: "default", name: "Зал", tableCount: 20 }]; // [{id, name, tableCount}]
    this.unavailable = {}; // key "kitchen|Name" or "bar|Name" -> {comment, disabledBy, disabledAt}
    this.pins = { waiter: "1111", cook: "1111", bartender: "1111", manager: "1111" };
    this.inventory = { kitchen: {}, bar: {} }; // dest -> { "Name": {qty, threshold} }
    this.pushSubs = []; // [{id, role, staffId, subscription}]
    this.actionLog = []; // [{ts, role, staffName, action, details}]
    this.ready = this.state.blockConcurrencyWhile(async () => {
      const storedOrders = await this.state.storage.get("orders");
      const storedHistory = await this.state.storage.get("history");
      const storedOpen = await this.state.storage.get("openTables");
      const storedClosed = await this.state.storage.get("closedTables");
      const storedStaff = await this.state.storage.get("staff");
      const storedRooms = await this.state.storage.get("rooms");
      const storedTableCount = await this.state.storage.get("tableCount"); // legacy, pre-rooms
      const storedUnavailable = await this.state.storage.get("unavailable");
      const storedPins = await this.state.storage.get("pins");
      const storedInventory = await this.state.storage.get("inventory");
      const storedPushSubs = await this.state.storage.get("pushSubs");
      const storedActionLog = await this.state.storage.get("actionLog");
      if (storedOrders) this.orders = storedOrders;
      if (storedHistory) this.history = storedHistory;
      if (storedOpen) this.openTables = storedOpen;
      if (storedClosed) this.closedTables = storedClosed;
      if (storedStaff) this.staff = storedStaff;
      if (storedRooms) this.rooms = storedRooms;
      else if (storedTableCount) this.rooms = [{ id: "default", name: "Зал", tableCount: storedTableCount }];
      if (storedUnavailable) this.unavailable = storedUnavailable;
      if (storedPins) this.pins = storedPins;
      if (storedInventory) this.inventory = storedInventory;
      if (storedPushSubs) this.pushSubs = storedPushSubs;
      if (storedActionLog) this.actionLog = storedActionLog;
    });
  }

  async persist() {
    await this.state.storage.put("orders", this.orders);
    await this.state.storage.put("history", this.history);
    await this.state.storage.put("openTables", this.openTables);
    await this.state.storage.put("closedTables", this.closedTables);
    await this.state.storage.put("staff", this.staff);
    await this.state.storage.put("rooms", this.rooms);
    await this.state.storage.put("unavailable", this.unavailable);
    await this.state.storage.put("pins", this.pins);
    await this.state.storage.put("inventory", this.inventory);
    await this.state.storage.put("pushSubs", this.pushSubs);
    await this.state.storage.put("actionLog", this.actionLog);
  }

  log(conn, action, details) {
    this.actionLog.push({
      ts: Date.now(),
      role: conn.role || null,
      staffName: conn.staffName || null,
      action,
      details: details || {},
    });
    if (this.actionLog.length > 1000) this.actionLog = this.actionLog.slice(-1000);
  }

  stateSnapshot() {
    return {
      type: "state",
      orders: this.orders,
      history: this.history,
      openTables: this.openTables,
      closedTables: this.closedTables,
      staff: this.staff,
      rooms: this.rooms,
      tableCount: this.rooms.reduce((s, r) => s + (r.tableCount || 0), 0), // kept for any old client still reading it
      unavailable: this.unavailable,
      inventory: this.inventory,
      actionLog: this.actionLog,
      vapidPublicKey: this.env.VAPID_PUBLIC_KEY || null,
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
    this.pushToRole(role, message);
  }

  pushText(message) {
    if (message.kind === "low_stock") return { title: "GO pub — заканчивается", body: `${message.name} — осталось ${message.qty}` };
    if (message.part) return { title: "GO pub — готово", body: `Стол ${message.table} (${message.part === "kitchen" ? "кухня" : "бар"})` };
    if (message.table !== undefined) return { title: "GO pub — новый заказ", body: `Стол ${message.table}` };
    return { title: "GO pub", body: "Новое уведомление" };
  }

  pushToRole(role, message) {
    const subs = this.pushSubs.filter(p => p.role === role);
    if (subs.length === 0) return;
    const text = this.pushText(message);
    subs.forEach(p => {
      sendWebPush(p.subscription, text, this.env)
        .then(async (resp) => {
          if (resp && (resp.status === 404 || resp.status === 410)) {
            this.pushSubs = this.pushSubs.filter(x => x.id !== p.id);
            await this.persist();
          }
        })
        .catch(() => {});
    });
  }

  sendTo(conn, message) {
    try { conn.ws.send(JSON.stringify(message)); } catch (e) {}
  }

  consumeStock(dest, items, notifyRole) {
    (items || []).forEach(i => {
      const stock = this.inventory[dest][i.name];
      if (!stock) return; // not tracked — nothing to do
      stock.qty = Math.max(0, stock.qty - (i.qty || 1));
      if (stock.qty <= stock.threshold) {
        this.notify(notifyRole, { kind: "low_stock", name: i.name, qty: stock.qty });
      }
    });
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
          } else if (msg.role !== "manager") {
            // Waiter/cook/bartender can no longer fall back to a shared PIN —
            // a valid personal QR link (staffId) is required. This is what
            // makes deleting/reassigning a staff member actually revoke access.
            server.send(JSON.stringify({ type: "auth_error", reason: "staff_required" }));
            server.close(4001, "staff id required");
            this.sockets.delete(conn);
            return;
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

        if (msg.type === "register_push" && msg.pushId && msg.subscription) {
          this.pushSubs = this.pushSubs.filter(p => p.id !== msg.pushId);
          this.pushSubs.push({ id: msg.pushId, role: conn.role, staffId: conn.staffId || null, subscription: msg.subscription });
          await this.persist();
        }

        if (msg.type === "unregister_push" && msg.pushId) {
          this.pushSubs = this.pushSubs.filter(p => p.id !== msg.pushId);
          await this.persist();
        }

        if (msg.type === "set_staff_name") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff && conn.staffId === msg.staffId) {
            staff.name = String(msg.name || "").slice(0, 40);
            conn.staffName = staff.name;
            this.log(conn, "set_name", { name: staff.name });
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
            this.log(conn, "change_pin", { role: msg.role });
            await this.persist();
            this.sendTo(conn, { type: "pin_changed", role: msg.role });
          } else {
            this.sendTo(conn, { type: "pin_change_error", reason: "invalid" });
          }
        }

        if (msg.type === "create_staff" && conn.role === "manager") {
          const staff = { id: crypto.randomUUID(), role: msg.role, name: "" };
          this.staff.push(staff);
          this.log(conn, "create_staff", { role: msg.role });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "delete_staff" && conn.role === "manager") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          this.staff = this.staff.filter(s => s.id !== msg.staffId);
          this.log(conn, "delete_staff", { role: staff ? staff.role : null, name: staff ? staff.name : null });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "reassign_staff" && conn.role === "manager") {
          const staff = this.staff.find(s => s.id === msg.staffId);
          if (staff) {
            staff.id = crypto.randomUUID(); // old device's cached id stops matching anyone
            this.log(conn, "reassign_staff", { role: staff.role, name: staff.name });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "create_room" && conn.role === "manager") {
          const room = { id: crypto.randomUUID(), name: String(msg.name || "Зал").slice(0, 40), tableCount: 10 };
          this.rooms.push(room);
          this.log(conn, "create_room", { name: room.name });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "update_room" && conn.role === "manager") {
          const room = this.rooms.find(r => r.id === msg.roomId);
          if (room) {
            if (msg.name !== undefined) room.name = String(msg.name).slice(0, 40) || room.name;
            if (msg.tableCount !== undefined) {
              const n = parseInt(msg.tableCount, 10);
              if (n && n > 0 && n <= 200) room.tableCount = n;
            }
            this.log(conn, "update_room", { name: room.name, tableCount: room.tableCount });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "delete_room" && conn.role === "manager") {
          if (this.rooms.length > 1) {
            const room = this.rooms.find(r => r.id === msg.roomId);
            this.rooms = this.rooms.filter(r => r.id !== msg.roomId);
            this.log(conn, "delete_room", { name: room ? room.name : null });
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
          this.log(conn, "disable_items", { dest, names: (msg.items || []).map(i => i.name) });
          await this.persist();
          this.broadcast();
          this.notify("manager", { kind: "menu_disabled", dest, names: (msg.items || []).map(i => i.name) });
        }

        if (msg.type === "enable_items" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          (msg.names || []).forEach(name => { delete this.unavailable[dest + "|" + name]; });
          this.log(conn, "enable_items", { dest, names: msg.names || [] });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "set_stock" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          const qty = Math.max(0, parseInt(msg.qty, 10) || 0);
          const threshold = Math.max(0, parseInt(msg.threshold, 10) || 0);
          this.inventory[dest][msg.name] = { qty, threshold };
          this.log(conn, "set_stock", { dest, name: msg.name, qty, threshold });
          await this.persist();
          this.broadcast();
        }

        if (msg.type === "clear_stock" && (conn.role === "cook" || conn.role === "bartender")) {
          const dest = conn.role === "cook" ? "kitchen" : "bar";
          delete this.inventory[dest][msg.name];
          this.log(conn, "clear_stock", { dest, name: msg.name });
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
          this.log(conn, "new_order", { table, kitchenCount: kitchenItems.length, barCount: barItems.length });
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
            this.log(conn, "kitchen_accept", { table: order.table });
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
            this.log(conn, "bar_accept", { table: order.table });
            await this.persist();
            this.broadcast();
          }
        }

        if (msg.type === "kitchen_ready") {
          const order = this.orders.find(o => o.id === msg.orderId);
          if (order) {
            order.kitchenStatus = "ready";
            order.kitchenReadyAt = Date.now();
            this.consumeStock("kitchen", order.kitchenItems, "cook");
            this.log(conn, "kitchen_ready", { table: order.table });
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
            this.consumeStock("bar", order.barItems, "bartender");
            this.log(conn, "bar_ready", { table: order.table });
            await this.persist();
            this.broadcast();
            this.notify("waiter", { table: order.table, orderId: order.id, part: "bar" });
          }
        }

        if (msg.type === "cancel_order") {
          const order = this.orders.find(o => o.id === msg.orderId);
          this.orders = this.orders.filter(o => o.id !== msg.orderId);
          if (order) this.log(conn, "cancel_order", { table: order.table });
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
            this.log(conn, "served", { table: order.table });
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
          this.log(conn, "close_table", { table, total });

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
