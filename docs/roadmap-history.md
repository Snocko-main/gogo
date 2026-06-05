# Historical Roadmap Migration

The old `ROADMAP.md` was removed so `ROAD_TO_V1.md` could become the single
execution plan for work after `v0.1.0`.

Useful historical items from the removed roadmap have been carried forward as
follows:

| Historical area | Current home |
| --- | --- |
| Security fixes around headers, body limits, panic handling, JSON errors, WebSocket limits, and cookies | `docs/security-checklist.md`, plus the Middleware, WebSocket, and Config sections in `ROAD_TO_V1.md` |
| Production essentials such as config, graceful shutdown, lifecycle hooks, proxy trust, request introspection, file helpers, and custom 404/405 behavior | v0.2.0 and v0.3.0 sections in `ROAD_TO_V1.md` |
| Middleware production defaults, sessions, CSRF, JWT, rate limiting, and CORS behavior | v0.4.0 section in `ROAD_TO_V1.md` |
| WebSocket hub contracts, Redis adapter behavior, backpressure, auth, origin, subprotocol, and thread-safety work | v0.5.0 section in `ROAD_TO_V1.md` |
| Testing helpers, `HTTPAdapter`, and WebSocket test-client decisions | v0.6.0 section in `ROAD_TO_V1.md` |
| Performance baselines and native hot-path work | v0.7.0 section in `ROAD_TO_V1.md` |
| Operations docs, reverse proxy examples, graceful shutdown examples, and release readiness | v0.8.0 and v1 release sections in `ROAD_TO_V1.md` |

When opening new issues or assigning agents, use `ROAD_TO_V1.md` instead of
recreating `ROADMAP.md`.
