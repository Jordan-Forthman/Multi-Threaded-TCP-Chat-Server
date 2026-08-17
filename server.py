"""Multi-threaded TCP chat server.

Serves a line-oriented chat protocol over raw TCP sockets. Each client is
handled on its own thread; all shared state (online users, rooms, accounts)
is guarded by a single reentrant lock. Registered accounts persist to JSON
between restarts.

Run `python3 server.py --help` for options, or connect with the bundled
client: `python3 client.py`.
"""

from socket import *
import argparse
import hashlib
import hmac
import json
import os
import secrets
import threading

# Resolve data files relative to this file so the server can be started from
# any working directory.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GOODBYEMSGFILE = os.path.join(BASE_DIR, "goodbye.txt")
BEFORELOGINMSGFILE = os.path.join(BASE_DIR, "prelogin.txt")
USERSFILE = os.path.join(BASE_DIR, "users.json")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 4521

# Global variables to store the content of the message files
beforeLoginMsg = ''
goodbyeMsg = ''
helpMsg = """

Commands supported ([] optional field, <> required field):

  who                      # List all online users
  status [user]            # Display user information
  start <topic>            # Start a room for a topic
  rooms                    # List all current rooms
  join <room number>       # Join a room
  leave <room number>      # Leave a room
  shout <message>          # Broadcast <message> to everyone online
  tell <user> <message>    # Tell <message> only to the user
  info [Info txt]          # Change or show your information text
  quit                     # Logout
  exit                     # Logout
  block <user>             # Block a user
  unblock <user>           # Unblock a user
  say <room number> <msg>  # Broadcast <msg> to everyone in room <room number>
  help                     # Print this message
  register <user> <passwd> # Register a new user

"""
welcomeBanner = """

            %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            %                                         %
             %              Welcome to               %
              %         Internet Chat Server        %
             %                                        %
            %                                          %
            %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

"""

# Load the pre-login and goodbye messages from their respective files.
def loadMsgs():
    global beforeLoginMsg
    global goodbyeMsg
    
    with open(BEFORELOGINMSGFILE, "r") as f:
        beforeLoginMsg = f.read()
    with open(GOODBYEMSGFILE, "r") as f:
        goodbyeMsg = f.read()

lock = threading.RLock()  # Lock for thread-safe operations on shared data structures

registered_users = {}  # Dictionary to store registered users data (password, info, blocked users)
online_users = {}      # Dictionary to store currently online users' data (socket, rooms, etc.)
rooms = {}             # Dictionary to store active chat rooms (topic, leader, members)
next_room_id = 0       # Counter for generating unique room IDs
next_guest_id = 1      # Counter for generating unique handles for anonymous clients

# ------------------------------------------- Password hashing -------------------------------------------------

# Stored form: "scrypt$<n>$<r>$<p>$<salt_hex>$<hash_hex>". Embedding the
# parameters means the cost can be raised later without invalidating hashes
# that were written under the old settings.
SCRYPT_N = 2 ** 14   # CPU/memory cost; 16384 needs ~16 MiB per hash
SCRYPT_R = 8
SCRYPT_P = 1
SCRYPT_DKLEN = 32


def hash_password(password, salt=None, n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P):
    """Derive a salted scrypt hash, encoded together with its parameters."""
    if salt is None:
        salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=n, r=r, p=p, dklen=SCRYPT_DKLEN)
    return f"scrypt${n}${r}${p}${salt.hex()}${dk.hex()}"


def verify_password(password, stored):
    """Check `password` against a stored credential.

    Returns (ok, upgraded). `upgraded` is a replacement hash the caller should
    persist, or None. Accounts written before hashing existed hold a plaintext
    password: those verify once and are rewritten as a hash, so an existing
    users.json keeps working without anyone re-registering.
    """
    if not stored.startswith("scrypt$"):
        if hmac.compare_digest(password, stored):
            return True, hash_password(password)
        return False, None

    try:
        _, n, r, p, salt_hex, hash_hex = stored.split("$")
        candidate = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt_hex),
                                   n=int(n), r=int(r), p=int(p),
                                   dklen=len(hash_hex) // 2)
    except ValueError:
        # Malformed record: fail closed rather than letting anyone in.
        return False, None

    # Constant-time compare, so a wrong guess cannot be narrowed down by timing.
    return hmac.compare_digest(candidate.hex(), hash_hex), None


# -------------------------------------------Persistence functions---------------------------------------------

# Load registered users from JSON file (if it exists).
def load_users():
    if not os.path.exists(USERSFILE):
        return
    with open(USERSFILE, 'r') as f:
        data = json.load(f)
    with lock:
        for u, d in data.items():
            registered_users[u] = {
                'password': d['password'],
                'info': d['info'],
                'blocked': set(d['blocked'])  # set for efficient lookups
            }

# Save registered users data to a JSON file.
def save_users():
    with lock:  # Ensure thread-safe saving
        # Convert sets (like blocked users) to lists for JSON compatibility
        data = {u: {'password': d['password'], 'info': d['info'], 'blocked': list(d['blocked'])}
                for u, d in registered_users.items()}
        # Serialize the registered_users dictionary to 'users.json'.
        with open(USERSFILE, 'w') as f:
            json.dump(data, f)

# --------------------------------------------Persistence Functions End----------------------------------------------

class LineReader:
    """Buffers a socket's byte stream and hands back one complete line at a time.

    TCP is a stream, not a sequence of messages: a single recv() may return
    several commands at once (a scripted client sending fast) or only part of
    one. Reading a line per recv() therefore mangles input from anything but a
    human typing. This buffers instead, so framing is correct either way.
    """

    def __init__(self, sock, maxline=4096):
        self.sock = sock
        self.buf = b""
        self.maxline = maxline

    def readline(self):
        """Next line without its terminator, or None once the peer is done."""
        while b"\n" not in self.buf:
            try:
                chunk = self.sock.recv(1000)
            except OSError:
                return None
            if not chunk:
                # Peer closed; surrender any unterminated trailing data once.
                if self.buf:
                    line, self.buf = self.buf, b""
                    return line
                return None
            self.buf += chunk
            if len(self.buf) > self.maxline:
                # Don't let a client without newlines grow the buffer forever.
                line, self.buf = self.buf, b""
                return line
        line, _, self.buf = self.buf.partition(b"\n")
        return line


"""
Send all data to sock, return 1 if successful
-1 if failed (socket error)
"""
def mySendAll(sock, data):
    total_sent = 0
    data_length = len(data)

    try:
        while total_sent < data_length:
            sent = sock.send(data[total_sent:])
            if sent == 0:
                # Socket connection broken
                return -1
            total_sent += sent
    except Exception:
        print("Socket send error in mySendAll.\n")
        return -1
    return 1

# Clean up resources for a disconnecting user. Leave all rooms & close room if user was the leader.
def cleanup_user(userName, sock):
    with lock:
        if userName not in online_users:
            return
        user_rooms = list(online_users[userName]['rooms'])
        for r in user_rooms:
            if r in rooms:
                rooms[r]['members'].remove(userName)
                if userName == rooms[r]['leader']:
                    # Close room and notify members
                    topic = rooms[r]['topic']
                    for member in list(rooms[r]['members']):
                        if member in online_users:
                            online_users[member]['rooms'].discard(r)
                            mySendAll(online_users[member]['sock'], 
                                     f"!!system!!: Room {r}(topic: {topic}) closed\n".encode())
                    del rooms[r]
        del online_users[userName]

# Core command processing
def processCmd(userName, sock, cmd):
    if not cmd:
        return 0  # Continue if empty command
    # Clean command
    tmp = cmd.split()
    if not tmp:
        return 0
    command = tmp[0].lower()

    # Get actual username for messages (even if logged in as 'guest')
    user_data = online_users[userName]
    actual_name = user_data['actual_name']
    is_registered = user_data['is_registered']

    # --- GUEST MODE RESTRICTION ---
    if not is_registered and command not in ['register', 'quit', 'exit']:
        mySendAll(sock, b"Unsupported command\n")
        return 0
    
# --------------------------------------------- COMMAND HANDLING ------------------------------------------------

    if command == 'who':  # List all online users
        with lock:
            display_names = []
            for uname, data in online_users.items():
                if data['is_registered']:
                    display_names.append(data['actual_name'])
                else:
                    display_names.append(uname)
        msg = f"{len(online_users)} users online:\n" + ', '.join(display_names) + '\n'
        mySendAll(sock, msg.encode())

    elif command == 'status':  # Display user information
        # Only accepts two words. Specified username or none (defaults to self)
        if len(tmp) > 2:
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        # Specified or default
        target = tmp[1] if len(tmp) == 2 else actual_name
        with lock:
            found = False
            for uname, data in online_users.items():
                if (data['is_registered'] and data['actual_name'] == target) or uname == target:
                    info = data['info']
                    blocked_list = data['blocked']
                    msg = f"User: {target}\n"
                    msg += f"Info: {info if info else '-'}\n"
                    msg += "Blocked User(s):\n"
                    if blocked_list:
                        msg += ', '.join(blocked_list) + "\n"
                    msg += "online\n"
                    mySendAll(sock, msg.encode())
                    found = True
                    break
            if not found:
                mySendAll(sock, b"User does not exist or not online\n")

    elif command == 'start':  # Start a new room with a topic
        if len(tmp) < 2:  # At least one word for the topic
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        topic = ' '.join(tmp[1:])
        with lock:
            global next_room_id
            rid = next_room_id
            next_room_id += 1
            # Create room entry
            rooms[rid] = {'topic': topic, 'leader': userName, 'members': {userName}}
            online_users[userName]['rooms'].add(rid)
        mySendAll(sock, f"!!system!!: {actual_name} created room {rid}, topic: {topic}\n".encode())

    elif command == 'rooms':  # List all current rooms
        with lock:
            room_count = len(rooms)
            if room_count == 0:
                mySendAll(sock, b"No current rooms.\n")
                return 0
            msg = f"{room_count} rooms:\n"
            # Sort rooms by ID to match potential sample ordering
            sorted_rooms = sorted(rooms.items())
            for rid, rdata in sorted_rooms:
                members = list(rdata['members'])
                member_count = len(members)
                msg += f"Room {rid}, topic: {rdata['topic']}\n"
                msg += f"{member_count} Participant(s): {', '.join(members)}\n"
        mySendAll(sock, msg.encode())

    elif command == 'join':  # Join a specified room
        # Exactly one numeric argument required (room #)
        if len(tmp) != 2:
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        try:
            rid = int(tmp[1])
        except ValueError:
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        with lock:
            if rid not in rooms:
                mySendAll(sock, b"Room does not exist\n")
                return 0
            if userName in rooms[rid]['members']:
                mySendAll(sock, f"You are already in Room {rid}.\n".encode())
                return 0
            # Add user to room and room to users data
            rooms[rid]['members'].add(userName)
            online_users[userName]['rooms'].add(rid)
        mySendAll(sock, f"You joined Room {rid}.\n".encode())

    elif command == 'leave':  # Leave a specified room
        # Exactly one numeric argument required (room #)
        if len(tmp) != 2: 
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        try:
            rid = int(tmp[1])
        except ValueError:
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        with lock:
            # Validation
            if rid not in rooms or userName not in rooms[rid]['members']:
                mySendAll(sock, b"Room does not exist or not in room\n")
                return 0
            # Remove user from room and room from users data
            rooms[rid]['members'].remove(userName)
            online_users[userName]['rooms'].remove(rid)
            if userName == rooms[rid]['leader']:
                # Close room if leader leaves
                topic = rooms[rid]['topic']
                for mem in list(rooms[rid]['members']):
                    online_users[mem]['rooms'].discard(rid)
                    mySendAll(online_users[mem]['sock'], 
                             f"!!system!!: Room {rid}(topic: {topic}) closed\n".encode())
                mySendAll(sock, f"!!system!!: Room {rid}(topic: {topic}) closed\n".encode())
                del rooms[rid]
                return 0
        mySendAll(sock, f"You left Room {rid}.\n".encode())

    elif command == 'shout':  # Broadcast message to all online users (except blocked)
        if len(tmp) < 2:  # Requires at least 2 arguments for message to broadcast
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        message = ' '.join(tmp[1:])
        with lock:
            for u, udata in list(online_users.items()):
                if u == userName:  # Skip self for now; echo separately
                    continue
                if actual_name in udata['blocked']:  # Respect blocks
                    mySendAll(udata['sock'], f"You have been blocked by {actual_name}\n".encode())
                    continue
                mySendAll(udata['sock'], f"!!{actual_name}!!: {message}\n".encode())
        # Echo to sender
        mySendAll(sock, f"!!{actual_name}!!: {message}\n".encode())

    elif command == 'tell':  # Send private message to a user (if not blocked)
        if len(tmp) < 3:  # Requires a username and a message
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        target = tmp[1]
        message = ' '.join(tmp[2:])
        with lock:
            # Validation
            found = False
            for uname, data in online_users.items():
                if (data['is_registered'] and data['actual_name'] == target) or uname == target:
                    if uname == userName:
                        mySendAll(sock, b"Cannot tell yourself\n")
                        return 0
                    if actual_name in data['blocked']:
                        mySendAll(sock, f"You have been blocked by {target}\n".encode())
                        return 0
                    mySendAll(data['sock'], f"{actual_name}: {message}\n".encode())
                    found = True
                    break
            if not found:
                mySendAll(sock, b"User is not online\n")

    elif command == 'info':  # Set or show user info
        with lock: 
            if len(tmp) == 1:  # No args
                info = online_users[userName]['info']
                msg = f"Info: {info}\n" if info else "Info: -\n"
                mySendAll(sock, msg.encode())
                return 0
            text = ' '.join(tmp[1:])
            online_users[userName]['info'] = text
            # Validation
            if is_registered:
                registered_users[actual_name]['info'] = text
                save_users()
        mySendAll(sock, b"Your information has been updated\n")

    elif command == 'block':  # Block a user
        if len(tmp) != 2:  # 1 arg to specify user
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        target = tmp[1]
        with lock:
            # Validation
            if target == actual_name:
                mySendAll(sock, b"Cannot block yourself\n")
                return 0
            found = False
            for uname, data in online_users.items():
                if (data['is_registered'] and data['actual_name'] == target) or uname == target:
                    online_users[userName]['blocked'].add(target)
                    if is_registered:
                        registered_users[actual_name]['blocked'].add(target)
                        save_users()
                    mySendAll(sock, f"User {target} has been blocked.\n".encode())
                    found = True
                    break
            if not found and target not in registered_users:
                mySendAll(sock, b"User does not exist\n")
        
    elif command == 'unblock':  # Unblock a user
        if len(tmp) != 2:  # 1 arg to specify a user
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        target = tmp[1]
        with lock:
            # Validation
            if target not in online_users[userName]['blocked']:
                mySendAll(sock, b"User not blocked\n")
                return 0
            online_users[userName]['blocked'].remove(target)
            if is_registered:
                registered_users[actual_name]['blocked'].remove(target)
                save_users()
            mySendAll(sock, f"User {target} has been unblocked.\n".encode())

    elif command == 'say':  # Send message to a room (if member)
        if len(tmp) < 3:  # 2 args for room number and message
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        try:
            rid = int(tmp[1])
        except ValueError:
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        message = ' '.join(tmp[2:])
        with lock:
            # Validation
            if rid not in rooms:
                mySendAll(sock, b"Room does not exist\n")
                return 0
            if userName not in rooms[rid]['members']:
                mySendAll(sock, b"Attempting to speak in a room without being a member of that room\n")
                return 0
            for mem in list(rooms[rid]['members']):
                mem_data = online_users[mem]
                if actual_name in mem_data['blocked']:
                    mySendAll(mem_data['sock'], f"You have been blocked by {actual_name}\n".encode())
                    continue
                mySendAll(mem_data['sock'], f"[Room {rid}] *{actual_name}*: {message}\n".encode())
                    
    elif command == 'help':  # Display help message with all commands
        mySendAll(sock, helpMsg.encode())

    elif command == 'register':  # Register a new user
        if len(tmp) != 3:  # Exactly 2 args: username and password
            mySendAll(sock, b"Incorrect command format\n")
            return 0
        u = tmp[1]
        p = tmp[2]
        with lock:
            # Validation
            if u in registered_users:
                mySendAll(sock, b"User already exists\n")
                return 0
        # Hashing is slow by design; do it before taking the lock back.
        hashed = hash_password(p)
        with lock:
            if u in registered_users:  # re-check: another thread may have won
                mySendAll(sock, b"User already exists\n")
                return 0
            registered_users[u] = {'password': hashed, 'info': '', 'blocked': set()}
            save_users()
        mySendAll(sock, f"User {u} registered\n".encode())
    else:
        # Handle unknown commands
        mySendAll(sock, b"Unsupported command\n")
    return 0

# --------------------------------------------- COMMAND HANDLING END ------------------------------------------------


# Manage lifecycle of client connection via independent thread
def handleOneClient(sock): 
    mySendAll(sock, beforeLoginMsg.encode())
    mySendAll(sock, "Enter your username: ".encode())

    reader = LineReader(sock)

    # Check if any data received, if not then close socket/ kill thread
    data1 = reader.readline()
    if data1 is None:
        sock.close()
        return

    # Strip data for clean username
    raw_name = data1.decode(errors='replace').split(' ')[0]
    actual_name = raw_name.replace("\t", " ").replace("\n", "").replace("\r", "")

    # Determine login status. The password prompt below blocks on the network,
    # so it must happen outside the lock -- otherwise a single client sitting at
    # the prompt would stall every other thread in the server.
    is_registered = False
    prompt_name = "guest"

    with lock:
        known_user = actual_name in registered_users

    if known_user:
        mySendAll(sock, "Enter your password: ".encode())
        data2 = reader.readline()
        if data2 is None:
            sock.close()
            return
        password = data2.decode(errors='replace').strip()

        with lock:
            stored = registered_users.get(actual_name, {}).get('password')
        # Deriving the hash is deliberately slow, so keep it outside the lock.
        ok, upgraded = verify_password(password, stored) if stored else (False, None)
        if not ok:
            mySendAll(sock, b"Login error (username/password do not match)\n")
            sock.close()
            return

        if upgraded:
            # Migrating a legacy plaintext record now that we know it is valid.
            with lock:
                if actual_name in registered_users:
                    registered_users[actual_name]['password'] = upgraded
            save_users()
            print(f"Upgraded stored password for {actual_name} to scrypt")

        is_registered = True
        prompt_name = actual_name  # If logged in then use real name

    with lock:
        # Prevent duplicate logins (only for registered)
        if is_registered and prompt_name in online_users:
            mySendAll(sock, b"User already online\n")
            sock.close()
            return

        # Anonymous clients each need their own key, or a second guest would
        # overwrite the first one's entry in online_users.
        if not is_registered:
            global next_guest_id
            prompt_name = "guest" if next_guest_id == 1 else f"guest{next_guest_id}"
            while prompt_name in online_users:
                next_guest_id += 1
                prompt_name = f"guest{next_guest_id}"
            next_guest_id += 1
            actual_name = prompt_name

        # Create user data
        user_data = {
            'sock': sock,
            'is_registered': is_registered,
            'rooms': set(),
            'cmd_count': 0,
            'actual_name': actual_name,
            'info': registered_users[actual_name]['info'] if is_registered else '',
            'blocked': registered_users[actual_name]['blocked'].copy() if is_registered else set()
        }
        online_users[prompt_name] = user_data

    # Send welcome sequence
    if not is_registered:
        guest_msg = ("\nYou login as a guest. The only commands that you can use are \n"
                     "'register username password', 'exit', and 'quit'.\n")
        mySendAll(sock, guest_msg.encode())

    cmdCount = 0
    mySendAll(sock, welcomeBanner.encode())
    mySendAll(sock, helpMsg.encode())
    mySendAll(sock, f"<{prompt_name}:{cmdCount}> ".encode())

    # A client that vanishes mid-session (terminal closed, network dropped)
    # raises out of recv/send. Clean up in `finally` either way, so the user
    # never lingers in online_users as a ghost.
    try:
        while True:
            data = reader.readline()
            if data is None:
                print(f"Client closed connection ({prompt_name})")
                break

            cmd = data.decode(errors='replace').strip()
            tmp = cmd.split()
            if tmp:
                command = tmp[0].lower()
                if command in ['quit', 'exit']:
                    mySendAll(sock, goodbyeMsg.encode())
                    break
                else:
                    processCmd(prompt_name, sock, cmd)

            # Update and send command prompt
            cmdCount += 1
            with lock:
                if prompt_name in online_users:
                    online_users[prompt_name]['cmd_count'] = cmdCount
            mySendAll(sock, f"<{prompt_name}:{cmdCount}> ".encode())
    except OSError as e:
        print(f"Connection error for {prompt_name}: {e}")
    finally:
        cleanup_user(prompt_name, sock)
        sock.close()

# ------------------------------------------------ Server startup ---------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Multi-threaded TCP chat server.",
        epilog="Connect with: python3 client.py  (or: telnet 127.0.0.1 %d)" % DEFAULT_PORT,
    )
    parser.add_argument('--host', default=os.environ.get('CHAT_HOST', DEFAULT_HOST),
                        help="interface to bind (default: %(default)s; use 0.0.0.0 to accept "
                             "connections from other machines)")
    parser.add_argument('--port', type=int, default=int(os.environ.get('CHAT_PORT', DEFAULT_PORT)),
                        help="port to listen on (default: %(default)s)")
    return parser.parse_args()


def main():
    args = parse_args()
    loadMsgs()
    load_users()

    s = socket(AF_INET, SOCK_STREAM)
    # Without SO_REUSEADDR a restart inside the TCP TIME_WAIT window fails with
    # "Address already in use", which makes the server annoying to iterate on.
    s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

    try:
        s.bind((args.host, args.port))
    except OSError as e:
        raise SystemExit(f"Cannot bind {args.host}:{args.port} -- {e}")
    s.listen(5)

    print(f"Internet Chat Server listening on {args.host}:{args.port}")
    print(f"Connect with:  python3 client.py --host {args.host} --port {args.port}")
    print("Press Ctrl-C to stop.")

    # Infinite loop to accept new clients
    try:
        while True:
            sock, addr = s.accept()
            print("Receive client connection from ", addr)
            p = threading.Thread(target=handleOneClient, args=(sock,), daemon=True)
            p.start()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        s.close()


if __name__ == "__main__":
    main()