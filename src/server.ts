const server = Bun.serve({
  async fetch(request, server) {
    server.upgrade(request);
  },

  websocket: {
    open(ws) {
      console.log(`client connected`);
    },
    close(ws) {
      console.log(`client disconnected`);
    },
    message(ws, message) {
      ws.send("sss");
      console.log(message);
    },
  },
});
console.log(`server started at ${server.url}`);
