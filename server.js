const { WebSocketServer } = require("ws");
const PORT = process.env.PORT || 8787;
const wss = new WebSocketServer({ port: PORT });
const rooms = new Map(); // roomCode -> { host, companions: Map<id,{ws,name}> }

const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };

wss.on("connection", (ws) => {
  ws.role = null; ws.roomCode = null; ws.clientId = null;
  ws._times = [];

  ws.on("message", (data) => {
    if (data.length > 8192) return;                  // payload grande: descarta
    const now = Date.now();                          // rate-limit ~40 msg/s
    ws._times = ws._times.filter((t) => now - t < 1000);
    if (ws._times.length > 40) return;
    ws._times.push(now);

    let msg; try { msg = JSON.parse(data.toString()); } catch { return; }

    if (msg.type === "join") {
      const code = String(msg.roomCode || "").trim().toUpperCase();
      if (!code) return send(ws, { type: "error", message: "Código da sala vazio." });
      ws.roomCode = code;
      if (msg.role === "host") {
        ws.role = "host";
        let room = rooms.get(code) || { host: null, companions: new Map() };
        room.host = ws; rooms.set(code, room);
        for (const [id, c] of room.companions) send(ws, { type: "participant_joined", id, name: c.name });
        send(ws, { type: "host_ready", roomCode: code });
      } else {
        ws.role = "companion";
        ws.clientId = String(msg.clientId || ("remote_" + now));
        const room = rooms.get(code);
        if (!room || !room.host) return send(ws, { type: "error", message: "Sala não encontrada ou host offline." });
        room.companions.set(ws.clientId, { ws, name: msg.displayName || "Participante" });
        send(ws, { type: "welcome", remoteUserId: ws.clientId, roomId: code });
        send(room.host, { type: "participant_joined", id: ws.clientId, name: msg.displayName || "Participante" });
      }
      return;
    }

    const room = rooms.get(ws.roomCode);
    if (!room) return;
    if (ws.role === "companion" && msg.type === "speaking") {
      const c = room.companions.get(ws.clientId);
      send(room.host, { type: "speaking", id: ws.clientId, name: c && c.name, isSpeaking: !!msg.isSpeaking, volume: Number(msg.volume) || 0 });
    }
  });

  ws.on("close", () => {
    const room = rooms.get(ws.roomCode);
    if (!room) return;
    if (ws.role === "companion") {
      room.companions.delete(ws.clientId);
      send(room.host, { type: "participant_left", id: ws.clientId });
    } else if (ws.role === "host" && room.host === ws) {
      room.host = null;
      for (const [, c] of room.companions) send(c.ws, { type: "error", message: "O host encerrou a sala." });
    }
    if (!room.host && room.companions.size === 0) rooms.delete(ws.roomCode);
  });
});
console.log("Nokotuber relay rodando na porta " + PORT);