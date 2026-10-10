const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
app.use(express.static("public"));

const wss = new WebSocket.Server({ server });
const rooms = new Map();
let counter = 0;

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function leave(ws) {
  if (!ws.room || !rooms.has(ws.room)) { ws.room = null; return; }
  const room = rooms.get(ws.room);
  room.delete(ws);
  for (const c of room) send(c, { type: "peer-left", id: ws.id });
  if (room.size === 0) rooms.delete(ws.room);
  ws.room = null;
}

wss.on("connection", (ws) => {
  ws.id = "p" + (++counter);
  ws.isAlive = true;
  ws.room = null;
  ws.role = null;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }
    if (msg.type === "ping") return;

    if (msg.type === "join") {
      leave(ws);
      const name = String(msg.room || "").trim();
      if (!name) return;
      ws.room = name;
      ws.role = msg.role === "camera" ? "camera" : "viewer";
      if (!rooms.has(name)) rooms.set(name, new Set());
      const room = rooms.get(name);
      const peers = [];
      for (const c of room) peers.push({ id: c.id, role: c.role });
      room.add(ws);
      send(ws, { type: "joined", id: ws.id, peers });
      for (const c of room) {
        if (c !== ws) send(c, { type: "peer-joined", id: ws.id, role: ws.role });
      }
      return;
    }

    if (!ws.room || !rooms.has(ws.room) || !msg.to) return;
    for (const c of rooms.get(ws.room)) {
      if (c.id === msg.to) {
        msg.from = ws.id;
        send(c, msg);
      }
    }
  });

  ws.on("close", () => leave(ws));
});

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(process.env.PORT || 3000, "0.0.0.0", () => {
  console.log("Server running");
});
