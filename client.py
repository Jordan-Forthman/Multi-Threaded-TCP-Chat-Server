"""Minimal chat client for the multi-threaded TCP chat server.

The server speaks a plain line-oriented protocol, so `telnet` or `nc` work
just as well. This client exists so the project has no external dependency:
telnet ships disabled on current macOS and Windows.

One thread pumps socket -> stdout so messages from other users arrive while
you are mid-keystroke; the main thread pumps stdin -> socket.

    python3 client.py --host 127.0.0.1 --port 4521
"""

import argparse
import socket
import sys
import threading

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 4521


def receive_loop(sock, stop):
    """Print everything the server sends until it closes the connection."""
    while not stop.is_set():
        try:
            data = sock.recv(4096)
        except OSError:
            break
        if not data:
            break
        # Server prompts ("<user:3> ") arrive without a trailing newline, so
        # write straight through and flush rather than using print().
        sys.stdout.write(data.decode(errors='replace'))
        sys.stdout.flush()

    stop.set()
    sys.stdout.write("\n[disconnected from server]\n")
    sys.stdout.flush()


def main():
    parser = argparse.ArgumentParser(description="Chat client for the TCP chat server.")
    parser.add_argument('--host', default=DEFAULT_HOST, help="server host (default: %(default)s)")
    parser.add_argument('--port', type=int, default=DEFAULT_PORT,
                        help="server port (default: %(default)s)")
    args = parser.parse_args()

    try:
        sock = socket.create_connection((args.host, args.port), timeout=10)
    except OSError as e:
        raise SystemExit(f"Could not connect to {args.host}:{args.port} -- {e}\n"
                         f"Is the server running?  python3 server.py")
    sock.settimeout(None)

    stop = threading.Event()
    reader = threading.Thread(target=receive_loop, args=(sock, stop), daemon=True)
    reader.start()

    interrupted = False
    try:
        for line in sys.stdin:
            if stop.is_set():
                break
            sock.sendall(line.rstrip("\n").encode() + b"\n")
    except KeyboardInterrupt:
        interrupted = True
    except (BrokenPipeError, OSError):
        pass

    # stdin hit EOF (Ctrl-D, or piped input running out). Half-close so the
    # server sees the end of our input, then let the reader drain the replies
    # still in flight before exiting -- otherwise piping commands in prints
    # nothing, since the reader is a daemon thread.
    if not interrupted and not stop.is_set():
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        reader.join(timeout=5)

    stop.set()
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


if __name__ == "__main__":
    main()
