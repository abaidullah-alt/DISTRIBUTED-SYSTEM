import json
import socket
import threading
import os

METADATA_FILE = "fsimage.json"
DATANODES = [5001, 5002, 5003]  # Added a 3rd node to allow replication
REPLICATION_FACTOR = 2
LOCK = threading.Lock()

# Load existing metadata from disk if it exists
if os.path.exists(METADATA_FILE):
    with open(METADATA_FILE, "r") as f:
        metadata = json.load(f)
    print(f"Loaded {len(metadata)} files from metadata disk.")
else:
    metadata = {}

def save_metadata():
    """Saves metadata to disk so files aren't lost on restart"""
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f)

def assign_nodes(chunk_index):
    """Assigns multiple nodes for Fault Tolerance (Replication)"""
    nodes = []
    for i in range(REPLICATION_FACTOR):
        nodes.append(DATANODES[(chunk_index + i) % len(DATANODES)])
    return nodes

def handle_client(conn, addr):
    try:
        request = conn.recv(4096).decode()
        parts = request.split(":")
        command = parts[0]
        
        if command == "UPLOAD":
            filename = parts[1]
            num_chunks = int(parts[2])
            
            chunks = []
            for i in range(num_chunks):
                ports = assign_nodes(i)
                chunks.append({"name": f"{filename}_part{i}", "ports": ports})
            
            with LOCK:
                metadata[filename] = {"chunks": chunks, "num_chunks": num_chunks}
                save_metadata()  # Save to disk immediately
            
            conn.send(json.dumps(chunks).encode())
            
        elif command == "DOWNLOAD":
            filename = parts[1]
            with LOCK:
                if filename in metadata:
                    conn.send(json.dumps(metadata[filename]["chunks"]).encode())
                else:
                    conn.send(b"FILE_NOT_FOUND")
        
        elif command == "LIST":
            with LOCK:
                conn.send(json.dumps(list(metadata.keys())).encode())
    
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        conn.close()

def start_namenode(port=5000):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', port))
    server.listen(5)
    print(f"🚀 NameNode running on port {port}...")
    
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    start_namenode()