import os
import socket
import json
import concurrent.futures
import streamlit as st

NAME_NODE_HOST = "localhost"
NAMENODE_PORT = 5000
DATANODE_PORTS = [5001, 5002, 5003]
CHUNK_SIZE = 1024 * 1024 * 64

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
INPUT_FOLDER = os.path.join(PROJECT_ROOT, "Input_Data")
OUTPUT_FOLDER = os.path.join(PROJECT_ROOT, "Downloaded_Data")

os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


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
        return None, f"Invalid metadata response: {exc}"


def get_file_metadata(filename):
    response, error = query_namenode(f"DOWNLOAD:{filename}")
    if error:
        return None, error

    if response == "FILE_NOT_FOUND":
        return None, "File not found"

    try:
        return json.loads(response), None
    except Exception as exc:
        return None, f"Invalid chunk metadata: {exc}"


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

    file_size = os.path.getsize(file_path)
    num_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

    response, error = query_namenode(f"UPLOAD:{filename}:{num_chunks}")
    if error:
        return False, f"NameNode error: {error}"

    try:
        chunks_info = json.loads(response)
    except Exception as exc:
        return False, f"NameNode returned invalid upload metadata: {exc}"

    results = []
    with open(file_path, "rb") as f, concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for chunk_info in chunks_info:
            chunk_name = chunk_info["name"]
            ports = chunk_info["ports"]
            data = f.read(CHUNK_SIZE)
            for port in ports:
                futures.append(executor.submit(upload_chunk_to_node, chunk_name, data, port))

        for future in concurrent.futures.as_completed(futures):
            success, message = future.result()
            results.append((success, message))

    success_all = all(success for success, _ in results)
    messages = [msg for _, msg in results]
    return success_all, "\n".join(messages)


def download_chunk_from_node(chunk_name, ports):
    for port in ports:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(("localhost", port))
                s.sendall(f"RETRIEVE:{chunk_name}".encode())
                data = b""
                while True:
                    packet = s.recv(1024 * 1024 * 4)
                    if not packet:
                        break
                    data += packet
            if data != b"FILE_NOT_FOUND":
                return data, f"Retrieved {chunk_name} from port {port}"
        except Exception:
            continue
    return None, f"Failed to retrieve {chunk_name} from ports {ports}"


def download_file(filename):
    chunks_info, error = get_file_metadata(filename)
    if error:
        return False, error

    downloaded_chunks = {}
    messages = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_to_index = {
            executor.submit(download_chunk_from_node, info["name"], info["ports"]): idx
            for idx, info in enumerate(chunks_info)
        }

        for future in concurrent.futures.as_completed(future_to_index):
            index = future_to_index[future]
            data, message = future.result()
            messages.append(message)
            if data is None:
                return False, "\n".join(messages)
            downloaded_chunks[index] = data

    download_path = os.path.join(OUTPUT_FOLDER, f"RESTORED_{filename}")
    with open(download_path, "wb") as f:
        for i in range(len(chunks_info)):
            f.write(downloaded_chunks[i])

    return True, f"Downloaded to: {download_path}\n" + "\n".join(messages)


st.set_page_config(page_title="Distributed File System Dashboard", layout="wide")
st.title("📊 Distributed File System Dashboard")

status_col, file_col = st.columns([1, 2])

with status_col:
    st.header("Cluster Status")

    namenode_status = "Online" if query_namenode("LIST")[0] is not None else "Offline"
    st.metric("NameNode", namenode_status)

    datanode_status = []
    for port in DATANODE_PORTS:
        alive, error = datanode_alive(port)
        datanode_status.append((port, alive, error))

    for port, alive, error in datanode_status:
        label = f"DataNode {port}"
        if alive:
            st.success(label + " is reachable")
        else:
            st.error(label + " is unavailable")
            st.caption(error)

    if st.button("Refresh cluster status"):
        st.experimental_rerun()

with file_col:
    st.header("Available files")
    files, list_error = list_files()
    if list_error:
        st.error(f"Could not query NameNode: {list_error}")
        files = []
    elif not files:
        st.info("No files stored in the cluster yet.")
    else:
        st.write(files)

    st.markdown("---")

    st.subheader("Input and Output folders")
    st.write(f"Input folder: `{INPUT_FOLDER}`")
    st.write(f"Download output folder: `{OUTPUT_FOLDER}`")

st.markdown("---")

upload_col, download_col = st.columns(2)

with upload_col:
    st.header("Upload a file")
    input_files = [f for f in os.listdir(INPUT_FOLDER) if os.path.isfile(os.path.join(INPUT_FOLDER, f))]
    if not input_files:
        st.warning("No files found in Input_Data. Place files in the Input_Data folder.")
    selected_upload = st.selectbox("Choose a file to upload", input_files)
    if st.button("Upload selected file") and selected_upload:
        with st.spinner("Uploading file to the distributed system..."):
            success, message = upload_file(selected_upload)
            if success:
                st.success("Upload completed successfully.")
            else:
                st.error("Upload experienced errors.")
            st.text(message)

with download_col:
    st.header("Download a file")
    selected_download = st.selectbox("Choose a stored file", files if files else [])
    if st.button("Download selected file") and selected_download:
        with st.spinner("Downloading file from the distributed system..."):
            success, message = download_file(selected_download)
            if success:
                st.success("Download completed successfully.")
            else:
                st.error("Download failed.")
            st.text(message)

st.markdown("---")
st.header("File metadata and chunk placement")
if files:
    chosen_file = st.selectbox("Select file for chunk metadata", files)
    if chosen_file:
        metadata, metadata_error = get_file_metadata(chosen_file)
        if metadata_error:
            st.error(metadata_error)
        else:
            chunk_table = []
            for chunk in metadata:
                chunk_table.append({
                    "Chunk Name": chunk["name"],
                    "DataNode Ports": ", ".join(str(p) for p in chunk["ports"])
                })
            st.table(chunk_table)
