import os
import json
import socket
import concurrent.futures
from flask import Flask, jsonify, render_template, request, redirect, url_for

NAME_NODE_HOST = "localhost"
NAMENODE_PORT = 5000
DATANODE_PORTS = [5001, 5002, 5003]
CHUNK_SIZE = 1024 * 1024 * 64

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
INPUT_FOLDER = os.path.join(PROJECT_ROOT, "Input_Data")
OUTPUT_FOLDER = os.path.join(PROJECT_ROOT, "Downloaded_Data")
STORAGE_NODE_DIRS = {
    5001: os.path.join(PROJECT_ROOT, "storage_node_5001"),
    5002: os.path.join(PROJECT_ROOT, "storage_node_5002"),
    5003: os.path.join(PROJECT_ROOT, "storage_node_5003"),
}

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def human_size(num_bytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def get_storage_stats(port):
    storage_dir = STORAGE_NODE_DIRS.get(port)
    chunk_count = 0
    total_size = 0
    if storage_dir and os.path.exists(storage_dir):
        for root, _, files in os.walk(storage_dir):
            for filename in files:
                chunk_count += 1
                file_path = os.path.join(root, filename)
                try:
                    total_size += os.path.getsize(file_path)
                except OSError:
                    continue
    return {
        "storage_dir": storage_dir,
        "chunk_count": chunk_count,
        "storage_used": total_size,
        "storage_used_human": human_size(total_size),
    }

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0


def query_namenode(command, timeout=3):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((NAME_NODE_HOST, NAMENODE_PORT))
            s.sendall(command.encode())
            response = b""
            while True:
                part = s.recv(65536)
                if not part:
                    break
                response += part
        return response.decode(), None
    except Exception as exc:
        return None, str(exc)


def datanode_alive(port, timeout=1):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect(("localhost", port))
        return True, None
    except Exception as exc:
        return False, str(exc)


def list_files():
    response, error = query_namenode("LIST")
    if error:
        return None, error
    try:
        return json.loads(response), None
    except Exception as exc:
        return None, str(exc)


def get_file_metadata(filename):
    response, error = query_namenode(f"DOWNLOAD:{filename}")
    if error:
        return None, error
    if response == "FILE_NOT_FOUND":
        return None, "File not found"
    try:
        return json.loads(response), None
    except Exception as exc:
        return None, str(exc)


def upload_chunk_to_node(chunk_name, chunk_data, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect(("localhost", port))
            s.sendall(f"STORE:{chunk_name}".encode())
            s.sendall(chunk_data)
        return True, f"Stored {chunk_name} on port {port}"
    except Exception as exc:
        return False, f"Failed {chunk_name} -> port {port}: {exc}"


def upload_file(filename):
    file_path = os.path.join(INPUT_FOLDER, filename)
    if not os.path.exists(file_path):
        return False, f"Input file not found: {filename}"
    size = os.path.getsize(file_path)
    num_chunks = (size + CHUNK_SIZE - 1) // CHUNK_SIZE

    response, error = query_namenode(f"UPLOAD:{filename}:{num_chunks}")
    if error:
        return False, f"NameNode error: {error}"

    try:
        chunks_info = json.loads(response)
    except Exception as exc:
        return False, f"NameNode returned invalid metadata: {exc}"

    results = []
    with open(file_path, "rb") as f, concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for chunk_info in chunks_info:
            chunk_name = chunk_info["name"]
            ports = chunk_info["ports"]
            chunk_data = f.read(CHUNK_SIZE)
            for port in ports:
                futures.append(executor.submit(upload_chunk_to_node, chunk_name, chunk_data, port))

        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    success = all(item[0] for item in results)
    messages = [item[1] for item in results]
    return success, "\n".join(messages)


def download_chunk_from_node(chunk_name, ports):
    for port in ports:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(("localhost", port))
                s.sendall(f"RETRIEVE:{chunk_name}".encode())
                data = b""
                while True:
                    part = s.recv(1024 * 1024 * 4)
                    if not part:
                        break
                    data += part
            if data != b"FILE_NOT_FOUND":
                return data, f"Retrieved {chunk_name} from port {port}"
        except Exception:
            continue
    return None, f"Failed to retrieve {chunk_name} from ports {ports}"


def download_file(filename):
    chunks_info, error = get_file_metadata(filename)
    if error:
        return False, error

    downloaded = {}
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(download_chunk_from_node, chunk["name"], chunk["ports"]): idx
            for idx, chunk in enumerate(chunks_info)
        }
        for future in concurrent.futures.as_completed(futures):
            index = futures[future]
            data, msg = future.result()
            messages.append(msg)
            if data is None:
                return False, "\n".join(messages)
            downloaded[index] = data

    output_path = os.path.join(OUTPUT_FOLDER, f"RESTORED_{filename}")
    with open(output_path, "wb") as outfile:
        for idx in range(len(chunks_info)):
            outfile.write(downloaded[idx])

    return True, f"Downloaded to {output_path}\n" + "\n".join(messages)


@app.route("/")
def index():
    available_input_files = [f for f in os.listdir(INPUT_FOLDER) if os.path.isfile(os.path.join(INPUT_FOLDER, f))]
    return render_template("index.html", input_files=available_input_files)


@app.route("/api/status")
def api_status():
    files, file_error = list_files()
    datanodes = []
    for port in DATANODE_PORTS:
        alive, error = datanode_alive(port)
        stats = get_storage_stats(port)
        datanodes.append({
            "port": port,
            "alive": alive,
            "error": error,
            "chunk_count": stats["chunk_count"],
            "storage_used": stats["storage_used"],
            "storage_used_human": stats["storage_used_human"],
            "storage_dir": stats["storage_dir"],
        })

    return jsonify({
        "namenode": {"alive": files is not None, "error": file_error, "file_count": len(files) if files else 0},
        "datanodes": datanodes,
        "files": files or []
    })


@app.route("/api/node-specs")
def api_node_specs():
    files, file_error = list_files()
    datanodes = []
    for port in DATANODE_PORTS:
        alive, error = datanode_alive(port)
        stats = get_storage_stats(port)
        datanodes.append({
            "port": port,
            "alive": alive,
            "error": error,
            "chunk_count": stats["chunk_count"],
            "storage_used": stats["storage_used"],
            "storage_used_human": stats["storage_used_human"],
            "storage_dir": stats["storage_dir"],
        })

    return jsonify({
        "namenode": {"file_count": len(files) if files else 0},
        "datanodes": datanodes,
    })


@app.route("/compare")
def compare_page():
    return redirect(url_for("index") + "#compare-view")


@app.route("/api/metadata/<filename>")
def api_metadata(filename):
    metadata, error = get_file_metadata(filename)
    if error:
        return jsonify({"error": error}), 400
    return jsonify({"metadata": metadata})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    data = request.get_json() or {}
    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "filename is required"}), 400
    success, message = upload_file(filename)
    return jsonify({"success": success, "message": message})


@app.route("/api/download", methods=["POST"])
def api_download():
    data = request.get_json() or {}
    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "filename is required"}), 400
    success, message = download_file(filename)
    return jsonify({"success": success, "message": message})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
