const uWS = require("uWebSockets.js");

const port = 3003;

uWS
  .App()
  .get("/plain", (res) => {
    res
      .writeStatus("200 OK")
      .writeHeader("Content-Type", "text/plain; charset=utf-8")
      .end("hello world\n");
  })
  .get("/json", (res) => {
    res
      .writeStatus("200 OK")
      .writeHeader("Content-Type", "application/json")
      .end('{"message":"hello world","ok":true}\n');
  })
  .get("/hello/:name", (res, req) => {
    res
      .writeStatus("200 OK")
      .writeHeader("Content-Type", "text/plain; charset=utf-8")
      .end(`hello ${req.getParameter(0)}\n`);
  })
  .listen(port, (token) => {
    if (!token) {
      console.error(`failed to listen on :${port}`);
      process.exit(1);
    }

    console.log(`uWebSockets.js listening on http://localhost:${port}`);
  });
