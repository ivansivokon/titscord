const { Server } = require('ws');

const wss = new Server({ noServer: true });
const clients = new Map();
let idCounter = 0;

wss.on('connection', (ws) => {
  const userId = ++idCounter;
  clients.set(userId, ws);
  ws.userId = userId;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      ws.name = msg.name || 'Anonymous';
      // Send existing users list to new client
      const users = [];
      clients.forEach((c, id) => {
        if (id !== userId) users.push({ id, name: c.name });
      });
      ws.send(JSON.stringify({ type: 'users', users }));
      // Notify others
      broadcast({ type: 'user-joined', id: userId, name: ws.name }, userId);
    } else if (msg.type === 'signal') {
      // Relay to target
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

module.exports = (req, res) => {
  if (req.headers.upgrade?.toLowerCase() === 'websocket') {
    wss.handleUpgrade(req, req.socket, Buffer.alloc(0), (ws) => {
      wss.emit('connection', ws, req);
    });
  } else {
    res.writeHead(426, { 'Content-Type': 'text/plain' });
    res.end('Upgrade Required');
  }
};