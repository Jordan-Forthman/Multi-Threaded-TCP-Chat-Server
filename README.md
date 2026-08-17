# Multi-Threaded TCP Chat Server

A concurrent chat server built on raw TCP sockets in pure Python — no
frameworks, no dependencies. Handles many simultaneous clients with a thread
per connection, supports persistent user accounts, private messaging, user
blocking, and dynamic chat rooms.

## Quickstart

Requires Python 3.8+. Nothing to install.

```bash
git clone https://github.com/Jordan-Forthman/Multi-Threaded-TCP-Chat-Server.git
cd Multi-Threaded-TCP-Chat-Server

python3 server.py            # terminal 1 — starts on 127.0.0.1:4521
python3 client.py            # terminal 2 — connect
python3 client.py            # terminal 3 — connect again to see it broadcast
```

Open two or more client terminals to watch messages fan out between them.
The server also speaks plain Telnet, so `telnet 127.0.0.1 4521` or
`nc 127.0.0.1 4521` work identically if you prefer.

### Options

```bash
python3 server.py --host 0.0.0.0 --port 9000   # accept connections from other machines
python3 client.py --host 192.168.1.20 --port 9000
```

Both also read `CHAT_HOST` / `CHAT_PORT` from the environment. The server
binds loopback by default so a fresh clone never opens a port to the network
unasked.

## Try it in 30 seconds

Start the server and two clients, then in the first client:

```
Enter your username: alice
> register alice hunter2      # create an account
> quit
```

Reconnect as `alice` (it will now prompt for the password) and, with a second
client connected as `bob`:

```
<alice:1> who                       # 2 users online: alice, bob
<alice:2> shout hello everyone      # bob sees: !!alice!!: hello everyone
<alice:3> start socket programming  # creates room 0, alice is leader
<alice:4> tell bob join room 0
<alice:5> say 0 anyone here?        # only room 0 members see this
```

Unregistered clients connect as guests and may only `register`, `quit`, or
`exit`. Run `help` at any prompt for the full command list.

## Commands

| Command | Description |
| --- | --- |
| `who` | List all online users |
| `status [user]` | Show a user's info, or your own |
| `start <topic>` | Create a room and become its leader |
| `rooms` | List active rooms and participants |
| `join <room>` / `leave <room>` | Enter or exit a room |
| `say <room> <msg>` | Message everyone in a room |
| `shout <msg>` | Broadcast to everyone online |
| `tell <user> <msg>` | Private message a user |
| `info [text]` | Show or set your info text |
| `block <user>` / `unblock <user>` | Manage your block list |
| `register <user> <pass>` | Create an account |
| `help` | Show commands |
| `quit` / `exit` | Log out |

## How it works

- **Thread per client.** The accept loop spawns a daemon `threading.Thread`
  per connection, so a slow or idle client never blocks the others.
- **One lock over shared state.** A `threading.RLock` guards online users,
  rooms, and accounts. The lock is never held across a blocking socket read —
  otherwise one client sitting at a password prompt would stall the server.
- **Reliable writes.** All output goes through a `sendall`-style helper that
  loops until the buffer drains, since a single `send()` may transmit only
  part of it.
- **Durable cleanup.** Session teardown runs in a `finally` block, so a client
  that vanishes without logging out still leaves rooms and frees its handle
  instead of lingering as a ghost user.
- **Persistence.** Accounts, info text, and block lists serialize to
  `users.json` on every change and reload at startup. Rooms and presence are
  intentionally runtime-only.

Full design notes — data structures, concurrency rules, per-function
responsibilities — are in [`docs/DESIGN.md`](docs/DESIGN.md).

## Known limitations

Passwords are stored in plaintext in `users.json`. That was the original
assignment's specification and is kept here so the implementation matches its
documented behavior; a production version would store a salted hash
(`hashlib.scrypt` in the standard library). The protocol is also unencrypted
plaintext over TCP, since it was designed to be Telnet-compatible.

## Project layout

```
server.py        chat server
client.py        client, so no telnet install is needed
prelogin.txt     login banner
goodbye.txt      logout message
docs/DESIGN.md   architecture and design notes
```

## Background

Originally built as a systems programming assignment (COP4521) exploring
socket programming and concurrency, then cleaned up to run anywhere: the
coursework version bound to the university host's `gethostname()` and assumed
a shared lab machine, so it would not start on a personal computer.
