const { Server } = require('ws');

// Инициализируем WebSocket-сервер один раз при первом вызове функции
if (!global._wssInitialized) {
  global._wssInitialized = true;

  const wss = new Server({ noServer: true });
  global.clients = new Map();
  let idCounter = 0;

  wss.on('connection', (ws) => {
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

  global._wss = wss;
}

module.exports = (req, res) => {
  // Если это не WebSocket-запрос — возвращаем 426
  if (req.headers['upgrade']?.toLowerCase() !== 'websocket') {
    res.statusCode = 426;
    res.setHeader('Content-Type', 'text/plain');
    res.end('Upgrade Required');
    return;
  }

  // Получаем серверный сокет Vercel и один раз вешаем обработчик upgrade
  const server = res.socket.server;
  if (!server._wsHandlerAttached) {
    server._wsHandlerAttached = true;
    server.on('upgrade', (request, socket, head) => {
      global._wss.handleUpgrade(request, socket, head, (ws) => {
        global._wss.emit('connection', ws, request);
      });
    });
  }

  // Ничего не отправляем — соединение будет передано WebSocket-серверу
};