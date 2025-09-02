const server = Bun.serve({
  async fetch(request, server) {
    server.upgrade(request);
  },

  websocket: {
    open(ws) {
      console.log(`client connected`);
    },
    message(ws, message) {
      console.log(message);
    },
  },
});
console.log(`server started at ${server.url}`);
