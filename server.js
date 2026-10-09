const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

app.use(express.static("public"));

const wss = new WebSocket.Server({ server });

const rooms = new Map();

wss.on("connection", (ws) => {

  let roomId = null;

  ws.on("message", (data) => {

    let message;

    try {
      message = JSON.parse(data.toString());
    } catch {
      return;
    }

    if (message.type === "join") {

      roomId = message.room;

      if (!rooms.has(roomId)) {
        rooms.set(roomId, new Set());
      }

      rooms.get(roomId).add(ws);

      return;
    }

    if (!roomId || !rooms.has(roomId)) {
      return;
    }

    for (const client of rooms.get(roomId)) {

      if (
        client !== ws &&
        client.readyState === WebSocket.OPEN
      ) {
        client.send(JSON.stringify(message));
      }

    }

  });

  ws.on("close", () => {

    if (roomId && rooms.has(roomId)) {

      rooms.get(roomId).delete(ws);

      if (rooms.get(roomId).size === 0) {
        rooms.delete(roomId);
      }

    }

  });

});

server.listen(3000, "0.0.0.0", () => {
  console.log("Server running on port 3000");
});

