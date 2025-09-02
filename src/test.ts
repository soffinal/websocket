import { WebSocket } from "./client";

const ws = new WebSocket("http://localhost:3000/");
const abort = ws.listen(() => {});
let i = 0;
setInterval(() => {
  ws.send(`hello ${i++}`);
  if (i === 2) {
    abort();
  }
}, 500);
