const { Server } = require('ws');

// Инициализируем WebSocket-сервер при первом обращении
if (!global._wss) {
  global._wss = new Server({ noServer: true });
  global.clients = new Map();
  let idCounter = 0;

  global._wss.on('connection', (ws) => {
    const userId = ++idCounter;
    global.clients.set(userId, ws);
    ws.userId = userId;

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      if (msg.type === 'join') {
        ws.name = msg.name || 'Anonymous';
        const users = [];
        global.clients.forEach((c, id) => {
          if (id !== userId) users.push({ id, name: c.name });
        });
        ws.send(JSON.stringify({ type: 'users', users }));
        broadcast({ type: 'user-joined', id: userId, name: ws.name }, userId);
      } else if (msg.type === 'signal') {
        const target = global.clients.get(msg.target);
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
      global.clients.delete(userId);
      broadcast({ type: 'user-left', id: userId });
    });

    ws.on('error', () => {});
  });

  function broadcast(data, exceptUserId = null) {
    const payload = JSON.stringify(data);
    global.clients.forEach((client, id) => {
      if (id !== exceptUserId && client.readyState === 1) {
        client.send(payload);
      }
    });
  }
}

module.exports = (req, res) => {
  const server = res.socket.server;

  // Обработчик upgrade вешаем только один раз на серверный сокет
  if (!server._wsHandlerAttached) {
    server._wsHandlerAttached = true;
    server.on('upgrade', (request, socket, head) => {
      global._wss.handleUpgrade(request, socket, head, (ws) => {
        global._wss.emit('connection', ws, request);
      });
    });
  }

  // Для serverless-функции на Vercel обязательно завершаем ответ
  res.end();
};