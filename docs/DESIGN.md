# Design Notes

Internal design of the chat server: the state it keeps, how concurrency is
kept safe, and what each moving part is responsible for.

## Overview

The server implements a chat service over raw TCP sockets. It supports user
registration, login (as guest or registered user), chat rooms, private
messaging, broadcasting, blocking, and a set of utility commands. It is
concurrent: every connected client is handled on its own thread, so no client
can block another. Registered accounts persist through JSON file storage, so
account information survives a restart.

The protocol is plain text and line-oriented, which makes it usable from any
raw TCP client — the bundled `client.py`, `telnet`, or `nc`.

## Data structures

| Structure | Purpose |
| --- | --- |
| `registered_users` | Persistent accounts. Key: username. Value: `password`, `info`, `blocked` (set of usernames). Loaded from and saved to `users.json`. |
| `online_users` | Runtime state for connected clients. Key: handle. Value: `sock`, `is_registered`, `rooms` (set of room IDs), `cmd_count`, `actual_name`, `info`, `blocked`. |
| `rooms` | Active rooms. Key: room ID (int). Value: `topic`, `leader`, `members` (set of handles). |
| `next_room_id` | Auto-incrementing counter for unique room IDs. |
| `next_guest_id` | Auto-incrementing counter for unique anonymous handles. |
| `lock` | `threading.RLock` guarding every one of the above. |

Sets and dicts give O(1) membership tests for the operations done most often
on every message — block checks and room membership checks.

## Concurrency model

One thread per client, spawned by the accept loop in `main()`. Threads are
daemons, so Ctrl-C tears the process down without hanging on live connections.

All shared state sits behind a single reentrant lock. The rule the code
follows is **never hold the lock across a blocking socket operation**: a
`recv` waits on a human typing, and holding the global lock across it would
stall every other thread in the process. The login path therefore reads
`registered_users` under the lock, releases it, prompts for and reads the
password, then re-acquires the lock to validate and publish the session. The
lock is reentrant because command handlers call helpers (`save_users`,
`cleanup_user`) that acquire it themselves.

Anonymous clients each get a distinct handle (`guest`, `guest2`, `guest3`, …).
They share a namespace with registered users in `online_users`, so a single
shared `"guest"` key would let one anonymous client evict another's session.

## Key functions

### `loadMsgs()`
Reads the pre-login banner and goodbye message from `prelogin.txt` and
`goodbye.txt` into globals at startup. Keeping this text out of the source
means it can be customized without touching code. Paths resolve relative to
the source file, so the server can be started from any working directory.

### `load_users()` / `save_users()`
Serialize `registered_users` to and from `users.json`. `save_users()` runs on
every change to persistent data (`register`, `info`, `block`, `unblock`),
converting `blocked` sets to lists for JSON compatibility. Runtime-only state
— rooms and online users — is deliberately not persisted.

### `mySendAll(sock, data)`
Sends all bytes of `data`, looping until the buffer is drained. A bare
`send()` may transmit only part of a buffer, which would corrupt chat output.
Returns 1 on success, -1 on socket error. Every write in the server goes
through it.

### `cleanup_user(userName, sock)`
Removes a user from `online_users` and from every room they were in. If they
led a room, the room closes and remaining members are notified. Called from a
`finally` block, so it runs on graceful logout, on a clean disconnect, and on
an abrupt drop (closed terminal, dropped network) alike — the case that would
otherwise leave ghost users and orphaned rooms behind.

### `processCmd(userName, sock, cmd)`
Parses and dispatches one command. Validates argument counts and formats,
enforces the guest restriction, checks block lists before every delivery, and
sends specific feedback for bad input rather than failing silently:

| Command | Behavior |
| --- | --- |
| `who` | List online users |
| `status [user]` | Show info for a user, or yourself |
| `start <topic>` | Create a room and become its leader |
| `rooms` | List rooms with participants |
| `join` / `leave` | Manage membership; a leader leaving closes the room |
| `shout <msg>` | Broadcast to everyone online, respecting blocks |
| `tell <user> <msg>` | Private message, respecting blocks |
| `say <room> <msg>` | Message a room you are a member of |
| `info [text]` | Show or set your info text; persists if registered |
| `block` / `unblock` | Manage your block list; persists if registered |
| `register <u> <p>` | Create an account |
| `help` | Print the command list |

Unknown input returns `Unsupported command`.

### `handleOneClient(sock)`
Owns one client session end to end: sends the pre-login banner, prompts for a
username, prompts for a password if that username is registered, rejects
duplicate logins, publishes the session into `online_users`, then loops
receiving commands and writing the `<user:n>` prompt. On `quit`/`exit`, on a
clean close, or on a socket error, it cleans up and closes.

### `main()`
Parses arguments, loads messages and accounts, binds the listening socket with
`SO_REUSEADDR` (so a restart inside the TCP `TIME_WAIT` window is not refused),
and runs the accept loop, spawning a thread per connection until interrupted.

## Error handling

Command handlers validate argument count and type before acting and return
specific messages (`Room does not exist`, `Incorrect command format`,
`User is not online`). Session threads catch `OSError` so one dropped client
cannot take down the process, and decode client bytes with `errors='replace'`
so malformed input cannot raise. Empty receives are treated as disconnects.

## Known limitations

Passwords are stored in plaintext in `users.json`, which is what the original
assignment specified. Production would use a salted hash — `hashlib.scrypt`
covers this in the standard library. There is no transport encryption; the
protocol is plaintext over TCP by design, since the assignment targeted
Telnet clients.
