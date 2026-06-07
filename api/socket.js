const { Server } = require('ws');

module.exports = (req, res) => {
  // Используем res.socket.server для хранения глобального WebSocket-сервера
  if (!res.socket.server.wss) {
    const wss = new Server({ noServer: true });
    res.socket.server.wss = wss;

    // Обработчик upgrade на уровне HTTP-сервера Vercel
    res.socket.server.on('upgrade', (request, socket, head) => {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    });

    // Логика WebSocket (аналогична предыдущей)
    let idCounter = 0;
    const clients = new Map();

    wss.on('connection', (ws) => {
      const userId = ++idCounter;
      clients.set(userId, ws);
      ws.userId = userId;

      ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }

        if (msg.type === 'join') {
          ws.name = msg.name || 'Anonymous';
          // Отправляем новичку список уже присутствующих
          const users = [];
          clients.forEach((c, id) => {
            if (id !== userId) users.push({ id, name: c.name });
          });
          ws.send(JSON.stringify({ type: 'users', users }));
          // Оповещаем остальных
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
        broadcast({ type: 'user-left', id: userId });
      });

      ws.on('error', () => {});
    });

    function broadcast(data, exceptUserId = null) {
      const payload = JSON.stringify(data);
      clients.forEach((client, id) => {
        if (id !== exceptUserId && client.readyState === 1) {
          client.send(payload);
        }
      });
    }
  }

  // Если запрос не на upgrade (обычный HTTP), возвращаем 426
  res.writeHead(426, { 'Content-Type': 'text/plain' });
  res.end('Upgrade Required');
};