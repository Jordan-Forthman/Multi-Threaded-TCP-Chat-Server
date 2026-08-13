from socket import *
import sys
import threading

GOODBYEMSGFILE = "./goodbye.txt"
BEFORELOGINMSGFILE = "./prelogin.txt"

beforeLoginMsg = ''
goodbyeMsg = ''

def loadMsgs():
    global beforeLoginMsg
    global goodbyeMsg
    
    with open(BEFORELOGINMSGFILE, "r") as f:
        beforeLoginMsg = f.read()
    with open(GOODBYEMSGFILE, "r") as f:
        goodbyeMsg = f.read()
                
n = len(sys.argv)
if (n != 2):
    print("Usage: server_port")
    exit()

loadMsgs()

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

    except Exception :
        print("Socket send error in mySendAll.\n")
        return -1

    return 1

def processCmd(userName, sock, cmd):
    print(f"process '{cmd}' from {userName}")

    # perform according to the cmd, echo for now
    mySendAll(sock, f"Server response to '{cmd}'\n".encode())

def handleOneClient(sock):

    mySendAll(sock, beforeLoginMsg.encode())
    mySendAll(sock, "Enter your username: ".encode())

    data1 = sock.recv(1000)
    if (len(data1) == 0) :
        sock.close()
        return
    
    data2 = data1.decode().split(' ')[0]
    userName = data2.replace("\t", " ").replace("\n", "").replace("\r", "")
       
    
    str = f"Welcome to the Internet Chat Room, {userName}!\n\n"
    mySendAll(sock, str.encode())

    cmdCount = 0
    mySendAll(sock, f"<{userName}:{cmdCount}> ".encode())
    
    while True:
        data = sock.recv(1000)
        if (len(data) == 0):
            print("Client closed connection")
            sock.close()
            break;

        cmd = data.decode().replace("\t", "").replace("\n", "").replace("\r", "")
        tmp = cmd.split()
        command = cmd.split()[0].lower()
        if (command == 'quit' or command == 'exit'):
            mySendAll(sock, goodbyeMsg.encode())
            sock.close()
            break
        else: 
            processCmd(userName, sock, cmd)

        # send prompt
        cmdCount = cmdCount + 1
        mySendAll(sock, f"<{userName}:{cmdCount}> ".encode())

s = socket()
h = gethostname()
print(sys.argv[0], sys.argv[1])

s.bind((h, int(sys.argv[1])))
s.listen(5)
        
while True:
    sock, addr = s.accept()
    print("Receive client connection from ", addr)
    p = threading.Thread(target=handleOneClient, args=(sock,), daemon = True)
    p.start()