import hashlib
import threading
import random
import logging
import unittest
import json
from flask import Flask, request, jsonify

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Node class representing a storage node
class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.files = {}

    def store_file(self, file_id, data):
        self.files[file_id] = data

    def get_file(self, file_id):
        return self.files.get(file_id, None)

    def delete_file(self, file_id):
        if file_id in self.files:
            del self.files[file_id]

# Distributed Storage System
class DistributedFileStorageSystem:
    def __init__(self, num_replicas=3):
        self.nodes = []
        self.file_map = {}
        self.num_replicas = num_replicas
        self.lock = threading.Lock()

    def add_node(self, node):
        with self.lock:
            self.nodes.append(node)
            logging.info(f"Node {node.node_id} added.")

    def remove_node(self, node_id):
        with self.lock:
            self.nodes = [node for node in self.nodes if node.node_id != node_id]
            logging.info(f"Node {node_id} removed.")

    def hash_file(self, data):
        return hashlib.sha256(data).hexdigest()

    def store_file(self, filename, data):
        file_id = self.hash_file(data)
        if file_id in self.file_map:
            logging.info(f"File {filename} already exists as {file_id}.")
            return file_id

        selected_nodes = random.sample(self.nodes, self.num_replicas)
        for node in selected_nodes:
            node.store_file(file_id, data)

        with self.lock:
            self.file_map[file_id] = {
                "filename": filename,
                "nodes": [node.node_id for node in selected_nodes]
            }
        logging.info(f"File {filename} stored as {file_id} in nodes {self.file_map[file_id]['nodes']}.")
        return file_id

    def retrieve_file(self, file_id):
        if file_id not in self.file_map:
            logging.error(f"File ID {file_id} not found.")
            return None

        for node_id in self.file_map[file_id]['nodes']:
            node = next((n for n in self.nodes if n.node_id == node_id), None)
            if node:
                data = node.get_file(file_id)
                if data:
                    return data

        logging.error(f"File ID {file_id} not retrievable from any nodes.")
        return None

    def delete_file(self, file_id):
        if file_id not in self.file_map:
            logging.error(f"File ID {file_id} not found.")
            return False

        for node_id in self.file_map[file_id]['nodes']:
            node = next((n for n in self.nodes if n.node_id == node_id), None)
            if node:
                node.delete_file(file_id)

        with self.lock:
            del self.file_map[file_id]
        logging.info(f"File ID {file_id} deleted from the system.")
        return True

# Initialize the distributed storage system
dfs = DistributedFileStorageSystem()

# RESTful API endpoints
@app.route('/add_node', methods=['POST'])
def add_node():
    node_id = request.json.get('node_id')
    dfs.add_node(Node(node_id))
    return jsonify({"status": "Node added", "node_id": node_id})

@app.route('/store', methods=['POST'])
def store_file():
    filename = request.json.get('filename')
    data = request.json.get('data').encode('utf-8')
    file_id = dfs.store_file(filename, data)
    return jsonify({"status": "File stored", "file_id": file_id})

@app.route('/retrieve/<file_id>', methods=['GET'])
def retrieve_file(file_id):
    data = dfs.retrieve_file(file_id)
    if data:
        return jsonify({"status": "File retrieved", "data": data.decode('utf-8')})
    return jsonify({"status": "File not found"}), 404

@app.route('/delete/<file_id>', methods=['DELETE'])
def delete_file(file_id):
    result = dfs.delete_file(file_id)
    if result:
        return jsonify({"status": "File deleted"})
    return jsonify({"status": "File not found"}), 404

class DistributedFileStorageSystemTestCase(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True
        # Clear the nodes and file map before each test
        dfs.nodes = []
        dfs.file_map = {}
        dfs.add_node(Node('node1'))
        dfs.add_node(Node('node2'))
        dfs.add_node(Node('node3'))

    def test_add_node(self):
        response = self.app.post('/add_node', json={'node_id': 'node4'})
        data = json.loads(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'Node added')
        self.assertEqual(data['node_id'], 'node4')

    def test_store_file(self):
        response = self.app.post('/store', json={'filename': 'test.txt', 'data': 'Hello World'})
        data = json.loads(response.data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(data['status'], 'File stored')
        self.assertIn('file_id', data)

    def test_retrieve_file(self):
        # Store a file first
        store_response = self.app.post('/store', json={'filename': 'test.txt', 'data': 'Hello World'})
        store_data = json.loads(store_response.data)
        file_id = store_data['file_id']

        # Retrieve the stored file
        retrieve_response = self.app.get(f'/retrieve/{file_id}')
        retrieve_data = json.loads(retrieve_response.data)
        self.assertEqual(retrieve_response.status_code, 200)
        self.assertEqual(retrieve_data['status'], 'File retrieved')
        self.assertEqual(retrieve_data['data'], 'Hello World')

    def test_delete_file(self):
        # Store a file first
        store_response = self.app.post('/store', json={'filename': 'test.txt', 'data': 'Hello World'})
        store_data = json.loads(store_response.data)
        file_id = store_data['file_id']

        # Delete the stored file
        delete_response = self.app.delete(f'/delete/{file_id}')
        delete_data = json.loads(delete_response.data)
        self.assertEqual(delete_response.status_code, 200)
        self.assertEqual(delete_data['status'], 'File deleted')

        # Try to retrieve the deleted file
        retrieve_response = self.app.get(f'/retrieve/{file_id}')
        self.assertEqual(retrieve_response.status_code, 404)
        retrieve_data = json.loads(retrieve_response.data)
        self.assertEqual(retrieve_data['status'], 'File not found')

if __name__ == '__main__':
    unittest.main()
