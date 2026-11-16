# ZeroClaw Board

A lightweight, read-only Kanban companion for ZeroClaw. Dynamic agent discovery, live WebSocket updates, English/Turkish UI, and light/dark themes.

## Run

Requires Python 3.11+ and a running, paired ZeroClaw gateway. The data adapter currently targets ZeroClaw 0.9.0's SQLite and trace schemas.

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
export ZEROCLAW_ROOT="$HOME/.zeroclaw"
export ZEROCLAW_GATEWAY="http://127.0.0.1:42617"
.venv/bin/python web.py
```

Open http://127.0.0.1:42618 and enter your existing gateway **bearer token**, not the one-time pairing code. For LAN binding set `BOARD_HOST` to your server's LAN address. `BOARD_PORT` defaults to 42618. No modifications to ZeroClaw or its running agents are needed.

For remote use, terminate TLS at a reverse proxy with WebSocket forwarding, set `BOARD_ORIGIN=https://your-board.example` and `BOARD_SECURE_COOKIE=1`. Do not send a gateway token over untrusted plain HTTP. Remote gateway URLs require HTTPS; HTTP is allowed only for loopback.

## How it works

- Reads configuration, cron jobs/runs, session metadata and control-plane tasks using read-only SQLite connections. Reads bounded recent runtime trace data, never sends raw arguments, outputs or conversation contents to the client.
- A shared collector checks sources every two seconds. WebSocket clients receive a snapshot only when the state changes. Ping/pong maintains connection health; browser reconnects with backoff.
- Token validation uses the gateway's authenticated `/api/status` endpoint. Tokens are held only in server memory; the browser gets an opaque HttpOnly, SameSite session cookie. Sessions expire after eight hours or a board restart. Open sockets revalidate every ten seconds and close if the gateway rejects the token or is unavailable.
- Login is rate-limited. WebSocket upgrades and login/logout require a matching Origin. There are no task mutation endpoints.
- Any valid gateway token can view this installation's full board. This is a single-owner companion, not a multi-tenant authorization layer.

## Honest status

Completed runs are execution results, not proof of project completion. Up to 30 recent cron runs within 24 hours are shown when their jobs still exist. Delegate activity inferred from logs is labeled; trace retention can limit visibility. The board does not recover stuck agents. Task names and agent aliases remain in their original language; interface text supports TR/EN.

## Development

```sh
.venv/bin/python -m unittest discover -s tests -v
node --check app.js
```

No frontend build step. `i18n.js` holds UI translations. `server.py` is the read-only ZeroClaw adapter; `auth.py` validates gateway tokens; `web.py` serves the UI and WebSocket stream.

## Roadmap

- Structured task lifecycle tools through a ZeroClaw plugin.
- Rich parent/subtask navigation when upstream lifecycle records provide relationships.
- Native event subscriptions when a stable ZeroClaw event API is available.

This is an independent companion project, not an official ZeroClaw plugin.
