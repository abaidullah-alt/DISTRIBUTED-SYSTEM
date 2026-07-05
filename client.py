import socket
import os
import json
import sys
import concurrent.futures

# --- YOUR CUSTOM STORAGE SETUP ---
BASE_PATH = r"c:\Users\Thisp\OneDrive\Desktop\Distributing system"
INPUT_FOLDER = os.path.join(BASE_PATH, "Input_Data")
OUTPUT_FOLDER = os.path.join(BASE_PATH, "Downloaded_Data")

# Ensure your folders exist
os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# 64MB chunks for large datasets!
CHUNK_SIZE = 1024 * 1024 * 64  
NAMENODE_HOST = 'localhost'
NAMENODE_PORT = 5000

def query_namenode(command):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((NAMENODE_HOST, NAMENODE_PORT))
        s.send(command.encode())
        response = s.recv(65536).decode()
        s.close()
        return response
    except Exception as e:
        print(f"NameNode connection error: {e}")
        return None

def upload_chunk_to_node(chunk_name, chunk_data, port):
    """Worker function to upload a single chunk"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', port))
        s.send(f"STORE:{chunk_name}".encode())
        s.sendall(chunk_data)
        s.close()
        return f"  ✓ {chunk_name} stored on port {port}"
    except Exception as e:
        return f"  ✗ Failed {chunk_name} on port {port}: {e}"

def upload(file_name):
    file_path = os.path.join(INPUT_FOLDER, file_name)
    if not os.path.exists(file_path):
        print(f"❌ File not found in Input_Data: {file_name}")
        return
    
    file_size = os.path.getsize(file_path)
    num_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
    
    print(f"\n📤 Uploading {file_name} ({file_size / (1024*1024):.2f} MB, {num_chunks} chunks)...")
    
    response = query_namenode(f"UPLOAD:{file_name}:{num_chunks}")
    if not response: return
    chunks_info = json.loads(response)
    
    # Upload chunks in parallel!
    with open(file_path, 'rb') as f, concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for chunk_info in chunks_info:
            chunk_name = chunk_info["name"]
            ports = chunk_info["ports"] # List of ports for replication
            
            chunk_data = f.read(CHUNK_SIZE)
            if not chunk_data: break
            
            # Send to ALL assigned replicated nodes
            for port in ports:
                futures.append(executor.submit(upload_chunk_to_node, chunk_name, chunk_data, port))
        
        for future in concurrent.futures.as_completed(futures):
            print(future.result())
            
    print(f"✅ Successfully uploaded {file_name}!")

def download_chunk_from_node(chunk_name, ports):
    """Worker function to download a chunk, trying replicated ports if one fails"""
    for port in ports:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('localhost', port))
            s.send(f"RETRIEVE:{chunk_name}".encode())
            
            data = b""
            while True:
                packet = s.recv(1024 * 1024 * 4)
                if not packet: break
                data += packet
                
            s.close()
            if data != b"FILE_NOT_FOUND":
                print(f"  ✓ Retrieved {chunk_name} from port {port}")
                return data
        except Exception:
            print(f"  ⚠ Port {port} failed for {chunk_name}, trying backup...")
            continue
    return None

def download(file_name):
    print(f"\n📥 Requesting {file_name} from NameNode...")
    response = query_namenode(f"DOWNLOAD:{file_name}")
    
    if not response or response == "FILE_NOT_FOUND":
        print("❌ File not found in the distributed system.")
        return
    
    chunks_info = json.loads(response)
    output_path = os.path.join(OUTPUT_FOLDER, f"RESTORED_{file_name}")
    
    # Download chunks in parallel!
    print(f"⚡ Downloading {len(chunks_info)} chunks in parallel...")
    downloaded_chunks = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_chunk = {executor.submit(download_chunk_from_node, info["name"], info["ports"]): i for i, info in enumerate(chunks_info)}
        
        for future in concurrent.futures.as_completed(future_to_chunk):
            index = future_to_chunk[future]
            data = future.result()
            if data:
                downloaded_chunks[index] = data
            else:
                print(f"❌ CRITICAL ERROR: Could not retrieve chunk {index} from ANY node.")
                return

    # Reconstruct file in correct order
    print("🔨 Reconstructing file...")
    with open(output_path, 'wb') as f:
        for i in range(len(chunks_info)):
            f.write(downloaded_chunks[i])
            
    print(f"✅ File successfully saved to: {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Commands: upload <filename> | download <filename> | list")
    elif sys.argv[1] == "upload" and len(sys.argv) == 3:
        upload(sys.argv[2])
    elif sys.argv[1] == "download" and len(sys.argv) == 3:
        download(sys.argv[2])
    elif sys.argv[1] == "list":
        print("Files in system:", query_namenode("LIST"))