"""
Name: Jordan Forthman
Date: 11/4/25
Assignment: 4
Due Date: 11/4/25

About this project: This project implements a multi-threaded internet chat server that supports user registration,
private messaging, room-based discussions, and message broadcasting with blocking functionality. It closely replicates the
behavior and output formatting of the FSU linprog chat server while ensuring thread-safe operations and persistent user data.

Assumptions: The server runs on a single machine and communicates with clients via plain Telnet over TCP.
Usernames and room topics contain no special characters that would interfere with command parsing.
The users.json file is accessible and writable in the servers working directory for persistence.

Testing Results:
register: pass
who: pass
status, info: pass
start, rooms, join, leave: pass
say: pass
shout: pass
tell: pass
block, unblock: pass
help, quit, exit: pass
error handling/feedbacks: pass
overall: pass
"""

from socket import *  
import sys
import threading
import json
import os

# Define file paths for pre-login and goodbye messages
GOODBYEMSGFILE = "./goodbye.txt"
BEFORELOGINMSGFILE = "./prelogin.txt"

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
                
# Check if the correct number of command-line arguments is provided
n = len(sys.argv)
if (n != 2):
    print("Usage: server_port")
    exit()

loadMsgs()

lock = threading.RLock()
registered_users = {}  # Dictionary to store registered users data (password, info, blocked users)
online_users = {}      # Dictionary to store currently online users data (socket, rooms, etc.)
rooms = {}             # Dictionary to store active chat rooms (topic, leader, members)
next_room_id = 0       # Counter for generating unique room IDs

# -------------------------------------------Persistence functions---------------------------------------------

# Load registered users from JSON file (if it exists).
if os.path.exists('users.json'):
    with open('users.json', 'r') as f:
        data = json.load(f)
        for u, d in data.items():
            registered_users[u] = {
                'password': d['password'],
                'info': d['info'],
                'blocked': set(d['blocked'])
            }

# Save registered users data to a JSON file.
def save_users():
    with lock:  # Ensure thread-safe saving
        # Convert sets (like blocked users) to lists for JSON compatibility
        data = {u: {'password': d['password'], 'info': d['info'], 'blocked': list(d['blocked'])} 
                for u, d in registered_users.items()}
        # Serialize the registered_users dictionary to 'users.json'.
        with open('users.json', 'w') as f:
            json.dump(data, f)

# --------------------------------------------Persistence Functions End----------------------------------------------

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

    # Guest Mode Restrictions
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
                if u == userName:  # Skip self
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
            if len(tmp) == 1:  # No args defaults to self
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
            registered_users[u] = {'password': p, 'info': '', 'blocked': set()}
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

    # Check if any data received, if not then close socket/ kill thread
    data1 = sock.recv(1000)
    if len(data1) == 0:
        sock.close()
        return
    
    # Strip data for clean username
    raw_name = data1.decode().split(' ')[0]
    actual_name = raw_name.replace("\t", " ").replace("\n", "").replace("\r", "")

    # Determine login status
    is_registered = False
    prompt_name = "guest"

    with lock:
        if actual_name in registered_users:
            mySendAll(sock, "Enter your password: ".encode())
            data2 = sock.recv(1000)
            if len(data2) == 0:
                sock.close()
                return
            password = data2.decode().strip()
            if password != registered_users[actual_name]['password']:
                mySendAll(sock, b"Login error (username/password do not match)\n")
                sock.close()
                return
            is_registered = True
            prompt_name = actual_name  # If logged in then use real name
        else:
            prompt_name = "guest"

        # Prevent duplicate logins (only for registered)
        if is_registered and prompt_name in online_users:
            mySendAll(sock, b"User already online\n")
            sock.close()
            return

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

    while True:
        data = sock.recv(1000)
        if len(data) == 0:
            print("Client closed connection")
            cleanup_user(prompt_name, sock)
            sock.close()
            break

        cmd = data.decode().strip()
        tmp = cmd.split()
        if tmp:
            command = tmp[0].lower()
            if command in ['quit', 'exit']:
                mySendAll(sock, goodbyeMsg.encode())
                cleanup_user(prompt_name, sock)
                sock.close()
                break
            else:
                processCmd(prompt_name, sock, cmd)

        # Update and send command prompt
        cmdCount += 1
        with lock:
            if prompt_name in online_users:
                online_users[prompt_name]['cmd_count'] = cmdCount
        mySendAll(sock, f"<{prompt_name}:{cmdCount}> ".encode())

# Main server setup
s = socket()
h = gethostname()
print(sys.argv[0], sys.argv[1])

s.bind((h, int(sys.argv[1])))
s.listen(5)
        
# Infinite loop to accept new clients
while True:
    sock, addr = s.accept()
    print("Receive client connection from ", addr)
    p = threading.Thread(target=handleOneClient, args=(sock,), daemon=True)
    p.start()