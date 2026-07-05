import os
import socket
import threading

from namenode import start_namenode
from datanode import start_datanode
from webui import app

DATANODE_PORTS = [5001, 5002, 5003]
NAMENODE_PORT = 5000


def port_is_open(port, host='localhost'):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        return sock.connect_ex((host, port)) == 0


def main():
    project_root = os.path.abspath(os.path.dirname(__file__))
    os.chdir(project_root)

    # Start NameNode in a background thread if port is free
    if port_is_open(NAMENODE_PORT):
        print(f"⚠ NameNode port {NAMENODE_PORT} is already in use. Assuming a NameNode is already running.")
    else:
        namenode_thread = threading.Thread(target=start_namenode, args=(NAMENODE_PORT,), daemon=True)
        namenode_thread.start()
        print(f"✅ NameNode thread started on port {NAMENODE_PORT}")

    # Start each DataNode in a background thread if its port is free
    for port in DATANODE_PORTS:
        storage_dir = os.path.join(project_root, f"storage_node_{port}")
        os.makedirs(storage_dir, exist_ok=True)
        if port_is_open(port):
            print(f"⚠ DataNode port {port} is already in use. Skipping start for this node.")
            continue
        thread = threading.Thread(target=start_datanode, args=(port, storage_dir), daemon=True)
        thread.start()
        print(f"✅ DataNode thread started on port {port} storing to {storage_dir}")

    print("✅ All services launching. Open http://localhost:8000 in your browser.")
    app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
