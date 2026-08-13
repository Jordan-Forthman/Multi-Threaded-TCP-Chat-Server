# Multi-Threaded TCP Chat Server

A concurrent Python internet chat server utilizing raw TCP sockets to support persistent user registration, private messaging, and dynamic chat rooms.

## Tech Stack
*   **Languages:** Python 3
*   **Libraries:** `socket`, `threading`, `json`, `os`

## Key Features & Learnings
*   **Concurrent Threading:** Engineered a multi-threaded architecture using `threading.Thread` to handle simultaneous client connections independently without blocking the main server loop. 
*   **Thread Safety:** Implemented `threading.RLock()` across all globally shared data structures (online users, rooms) to prevent race conditions during high-frequency concurrent read/write operations.
*   **State Persistence & Features:** Developed robust server functionalities including direct messaging (`tell`), broadcasting (`shout`), user blocking, and dynamic room creation, ensuring user states and credentials persist between server reboots via JSON serialization.

## How to Run
```bash
python3 assignment4.py <port_number>