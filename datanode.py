import socket
import os
import sys
import threading

# Increased buffer for faster 64MB chunk transfers
CHUNK_SIZE = 1024 * 1024 * 4  

def handle_client(conn, addr, storage_dir):
    try:
        command = conn.recv(1024).decode()
        
        if command.startswith("STORE:"):
            chunk_name = command.split(":", 1)[1]
            
            data = b""
            while True:
                chunk = conn.recv(CHUNK_SIZE)
                if not chunk:
                    break
                data += chunk
            
            file_path = os.path.join(storage_dir, chunk_name)
            with open(file_path, 'wb') as f:
                f.write(data)
            
            print(f"💾 Stored {chunk_name} ({len(data)} bytes)")
            
        elif command.startswith("RETRIEVE:"):
            chunk_name = command.split(":", 1)[1]
            file_path = os.path.join(storage_dir, chunk_name)
            
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    conn.sendall(f.read())
                print(f"📤 Sent {chunk_name}")
            else:
                conn.sendall(b"FILE_NOT_FOUND")
                
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        conn.close()

def start_datanode(port, storage_dir):
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('localhost', port))
    server.listen(10)
    print(f"📦 DataNode ready on port {port} | Saving to: {storage_dir}")
    
    try:
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr, storage_dir))
            thread.daemon = True
            thread.start()
    except KeyboardInterrupt:
        print(f"\nShutting down DataNode on port {port}...")
    finally:
        server.close()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
    storage_dir = sys.argv[2] if len(sys.argv) > 2 else f"./storage_node_{port}"
    start_datanode(port, storage_dir)