const { Server } = require('ws');

if (!global._wssInit) {
  global._wssInit = true;
  console.log('Initializing WebSocket server');

  const wss = new Server({ noServer: true });
  const clients = new Map();
  let idCounter = 0;

  function broadcast(data, exceptUserId = null) {
    const payload = JSON.stringify(data);
    clients.forEach((client, id) => {
      if (id !== exceptUserId && client.readyState === 1) {
        client.send(payload);
      }
    });
  }

  wss.on('connection', (ws) => {
    const userId = ++idCounter;
    clients.set(userId, ws);
    ws.userId = userId;
    console.log(`Client connected: ${userId}`);

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }
      console.log(`Message from ${userId}:`, msg.type);

      if (msg.type === 'join') {
        ws.name = msg.name || 'Anonymous';
        const users = [];
        clients.forEach((c, id) => {
          if (id !== userId) users.push({ id, name: c.name });
        });
        ws.send(JSON.stringify({ type: 'users', users }));
        broadcast({ type: 'user-joined', id: userId, name: ws.name }, userId);
      } else if (msg.type === 'signal') {
        const target = clients.get(msg.target);
        if (target && target.readyState === 1) {
          target.send(JSON.stringify({
            type: 'signal',
            sender: userId,
            data: msg.data
          }));
        }
      }
    });

    ws.on('close', () => {
      clients.delete(userId);
      console.log(`Client disconnected: ${userId}`);
      broadcast({ type: 'user-left', id: userId });
    });

    ws.on('error', (err) => {
      console.error(`WebSocket error for ${userId}:`, err.message);
    });
  });

  global._wss = wss;
  global._clients = clients;
  global._broadcast = broadcast;
}

module.exports = (req, res) => {
  const server = res.socket.server;
  if (!server._wsHandlerAttached) {
    server._wsHandlerAttached = true;
    server.on('upgrade', (request, socket, head) => {
      console.log('WebSocket upgrade request');
      global._wss.handleUpgrade(request, socket, head, (ws) => {
        global._wss.emit('connection', ws, request);
      });
    });
  }
  // Никакого res.end() — Vercel оставит соединение открытым
};