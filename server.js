const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// ── Practical 1: Camera streaming ──────────────────────────────
let streamerSocket = null;
let viewerSockets = [];

// ── Practical 3: Location / Mic / Device info ──────────────────
let p3PhoneSocket = null;
let p3ViewerSockets = [];

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

  // Phone side registers
  socket.on('p3-phone-ready', () => {
    p3PhoneSocket = socket;
    console.log('[P3] Phone connected');
    p3ViewerSockets.forEach(v => v.emit('p3-phone-available'));
  });

  // PC side registers
  socket.on('p3-viewer-ready', () => {
    p3ViewerSockets.push(socket);
    console.log('[P3] Viewer connected');
    if (p3PhoneSocket) socket.emit('p3-phone-available');
  });

  // Phone sends location
  socket.on('p3-location', (data) => {
    console.log('[P3] Location received:', data);
    p3ViewerSockets.forEach(v => v.emit('p3-location', data));
  });

  // Phone sends device info
  socket.on('p3-device-info', (data) => {
    console.log('[P3] Device info received');
    p3ViewerSockets.forEach(v => v.emit('p3-device-info', data));
  });

  // Phone sends clipboard
  socket.on('p3-clipboard', (data) => {
    console.log('[P3] Clipboard received');
    p3ViewerSockets.forEach(v => v.emit('p3-clipboard', data));
  });

  // Phone sends IP info
  socket.on('p3-ip-info', (data) => {
    console.log('[P3] IP info received');
    p3ViewerSockets.forEach(v => v.emit('p3-ip-info', data));
  });

  // WebRTC signaling for microphone stream (P3)
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
    // P1 cleanup
    if (socket === streamerSocket) {
      streamerSocket = null;
      viewerSockets.forEach(v => v.emit('streamer-offline'));
    } else {
      viewerSockets = viewerSockets.filter(v => v !== socket);
    }

    // P3 cleanup
    if (socket === p3PhoneSocket) {
      p3PhoneSocket = null;
      p3ViewerSockets.forEach(v => v.emit('p3-phone-offline'));
      console.log('[P3] Phone disconnected');
    } else {
      p3ViewerSockets = p3ViewerSockets.filter(v => v !== socket);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
