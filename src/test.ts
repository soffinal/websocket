import { filter } from "@soffinal/stream";
import { WebSocket } from "./client";

const ws = new WebSocket("http://localhost:3000/", { maxIdle: 3000 });

// const abort = ws.pipe(filter((ev) => ev.type === "message")).listen(console.log);

let i = 0;

setInterval(() => {
  if (i < 2) ws.send(`hello ${i++}`);
}, 500);
