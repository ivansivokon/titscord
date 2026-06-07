const { Server } = require('ws');

if (!global._wssInit) {
  global._wssInit = true;
  console.log('Initializing WebSocket server');

  const wss = new Server({ noServer: true });
  const clients = new Map();
  let idCounter = 0;

  // Безопасная отправка одному клиенту
  function safeSend(ws, data) {
    try {
      if (ws.readyState === 1) {  // OPEN
        ws.send(JSON.stringify(data));
      }
    } catch (err) {
      console.error('Send error:', err.message);
    }
  }

  // Безопасная массовая рассылка
  function broadcast(data, exceptUserId = null) {
    const payload = JSON.stringify(data);  // сериализуем один раз
    clients.forEach((client, id) => {
      if (id !== exceptUserId && client.readyState === 1) {
        try {
          client.send(payload);
        } catch (err) {
          console.error(`Broadcast error to ${id}:`, err.message);
        }
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
        // Список уже присутствующих
        const users = [];
        clients.forEach((c, id) => {
          if (id !== userId) users.push({ id, name: c.name });
        });
        safeSend(ws, { type: 'users', users });
        broadcast({ type: 'user-joined', id: userId, name: ws.name }, userId);
      } else if (msg.type === 'signal') {
        const target = clients.get(msg.target);
        if (target) {
          safeSend(target, {
            type: 'signal',
            sender: userId,
            data: msg.data
          });
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
      // При ошибке лучше явно закрыть сокет, если это нужно
      try { ws.close(); } catch (e) {}
      clients.delete(userId);
    });
  });

  global._wss = wss;
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
  // Не вызываем res.end() – соединение остаётся активным
};