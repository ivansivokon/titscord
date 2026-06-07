export default {
    async fetch(request) {
      // Принимаем только WebSocket-запросы
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Upgrade Required', { status: 426 });
      }
  
      // Создаём пару WebSocket-клиентов: один вернётся браузеру, второй останется на сервере
      const [client, server] = Object.values(new WebSocketPair());
  
      // Присоединяем серверную сторону к нашему чату
      handleConnection(server);
  
      // Возвращаем клиентскую сторону браузеру с апгрейдом протокола
      return new Response(null, { status: 101, webSocket: client });
    }
  };
  
  // Глобальное состояние (сохраняется между вызовами, пока активен Edge-воркер)
  let clients = new Map();
  let idCounter = 0;
  
  function handleConnection(ws) {
    const userId = ++idCounter;
    clients.set(userId, ws);
    ws.userId = userId;
  
    // Принимаем сообщения
    ws.addEventListener('message', (event) => {
      let msg;
      try { msg = JSON.parse(event.data); } catch { return; }
  
      if (msg.type === 'join') {
        ws.name = msg.name || 'Anonymous';
        // Отправляем новенькому список присутствующих
        const users = [];
        clients.forEach((c, id) => {
          if (id !== userId) users.push({ id, name: c.name });
        });
        safeSend(ws, JSON.stringify({ type: 'users', users }));
        // Оповещаем остальных
        broadcast({ type: 'user-joined', id: userId, name: ws.name }, userId);
      } else if (msg.type === 'signal') {
        const target = clients.get(msg.target);
        if (target) {
          safeSend(target, JSON.stringify({
            type: 'signal',
            sender: userId,
            data: msg.data
          }));
        }
      }
    });
  
    // Обрабатываем закрытие
    ws.addEventListener('close', () => {
      clients.delete(userId);
      broadcast({ type: 'user-left', id: userId });
    });
  
    // Ловим ошибки
    ws.addEventListener('error', (err) => {
      console.error(`Error on client ${userId}:`, err.message);
      try { ws.close(); } catch (e) {}
      clients.delete(userId);
    });
  }
  
  // Безопасная отправка с проверкой состояния
  function safeSend(ws, data) {
    try {
      if (ws.readyState === 1) { // OPEN
        ws.send(data);
      }
    } catch (err) {
      console.error('Send error:', err.message);
    }
  }
  
  // Массовая рассылка всем, кроме указанного
  function broadcast(data, exceptUserId = null) {
    const payload = JSON.stringify(data);
    clients.forEach((ws, id) => {
      if (id !== exceptUserId && ws.readyState === 1) {
        try {
          ws.send(payload);
        } catch (err) {
          console.error(`Broadcast error to ${id}:`, err.message);
        }
      }
    });
  }