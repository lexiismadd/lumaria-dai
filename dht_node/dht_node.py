#!/usr/bin/env python3
"""
Simplified Kademlia DHT Node
For distributed knowledge network
"""

import asyncio
import hashlib
import json
import random
import time
from collections import defaultdict
from datetime import datetime

class KademliaNode:
    """Simplified Kademlia DHT implementation"""
    
    def __init__(self, node_id=None, host='0.0.0.0', port=8000):
        # Generate random 160-bit node ID if not provided
        self.node_id = node_id or self.generate_node_id()
        self.host = host
        self.port = port
        
        # Routing table: distance -> list of nodes
        self.routing_table = defaultdict(list)
        
        # Data storage: key -> value
        self.storage = {}
        
        # Known peers
        self.peers = set()
        
        # Statistics
        self.stats = {
            'requests_handled': 0,
            'peers_discovered': 0,
            'data_stored': 0,
            'uptime_start': time.time()
        }
    
    @staticmethod
    def generate_node_id():
        """Generate random 160-bit node ID"""
        random_data = str(time.time()) + str(random.random())
        return int(hashlib.sha1(random_data.encode()).hexdigest(), 16)
    
    def distance(self, node_id_1, node_id_2):
        """XOR distance between two node IDs"""
        return node_id_1 ^ node_id_2
    
    def add_peer(self, peer_id, peer_host, peer_port):
        """Add a peer to routing table"""
        if peer_id == self.node_id:
            return  # Don't add ourselves
        
        peer_info = {
            'id': peer_id,
            'host': peer_host,
            'port': peer_port,
            'last_seen': time.time()
        }
        
        # Calculate distance bucket
        dist = self.distance(self.node_id, peer_id)
        bucket = dist.bit_length() - 1
        
        # Add to routing table (keep max 20 per bucket - k=20)
        self.routing_table[bucket].append(peer_info)
        if len(self.routing_table[bucket]) > 20:
            # Remove oldest
            self.routing_table[bucket] = sorted(
                self.routing_table[bucket],
                key=lambda x: x['last_seen'],
                reverse=True
            )[:20]
        
        self.peers.add((peer_host, peer_port))
        self.stats['peers_discovered'] += 1
    
    def find_closest_nodes(self, target_id, k=20):
        """Find k closest nodes to target ID"""
        all_nodes = []
        
        for bucket in self.routing_table.values():
            all_nodes.extend(bucket)
        
        # Sort by distance to target
        all_nodes.sort(key=lambda n: self.distance(n['id'], target_id))
        
        return all_nodes[:k]
    
    async def store(self, key, value):
        """Store key-value pair locally"""
        key_hash = int(hashlib.sha1(key.encode()).hexdigest(), 16)
        
        self.storage[key] = {
            'value': value,
            'stored_at': time.time(),
            'key_hash': key_hash
        }
        
        self.stats['data_stored'] += 1
        print(f"[STORE] Key: {key[:20]}... stored locally")
    
    async def retrieve(self, key):
        """Retrieve value by key"""
        if key in self.storage:
            return self.storage[key]['value']
        return None
    
    def get_stats(self):
        """Get node statistics"""
        uptime = time.time() - self.stats['uptime_start']
        
        return {
            'node_id': hex(self.node_id),
            'address': f"{self.host}:{self.port}",
            'uptime_seconds': int(uptime),
            'uptime_hours': round(uptime / 3600, 2),
            'peers_known': len(self.peers),
            'buckets_used': len(self.routing_table),
            'items_stored': len(self.storage),
            'total_requests': self.stats['requests_handled'],
            'total_peers_discovered': self.stats['peers_discovered']
        }
    
    async def handle_ping(self, request_data):
        """Handle PING request"""
        return {
            'type': 'PONG',
            'node_id': self.node_id,
            'host': self.host,
            'port': self.port,
            'timestamp': time.time()
        }
    
    async def handle_find_node(self, request_data):
        """Handle FIND_NODE request"""
        target_id = request_data.get('target_id')
        
        if target_id:
            closest = self.find_closest_nodes(target_id, k=20)
            return {
                'type': 'NODES',
                'nodes': closest,
                'timestamp': time.time()
            }
        
        return {'type': 'ERROR', 'message': 'No target_id provided'}
    
    async def handle_store(self, request_data):
        """Handle STORE request"""
        key = request_data.get('key')
        value = request_data.get('value')
        
        if key and value:
            await self.store(key, value)
            return {
                'type': 'STORED',
                'key': key,
                'timestamp': time.time()
            }
        
        return {'type': 'ERROR', 'message': 'Missing key or value'}
    
    async def handle_find_value(self, request_data):
        """Handle FIND_VALUE request"""
        key = request_data.get('key')
        
        if key:
            value = await self.retrieve(key)
            
            if value:
                return {
                    'type': 'VALUE',
                    'key': key,
                    'value': value,
                    'timestamp': time.time()
                }
            else:
                # Return closest nodes instead
                key_hash = int(hashlib.sha1(key.encode()).hexdigest(), 16)
                closest = self.find_closest_nodes(key_hash, k=20)
                return {
                    'type': 'NODES',
                    'nodes': closest,
                    'timestamp': time.time()
                }
        
        return {'type': 'ERROR', 'message': 'No key provided'}
    
    async def handle_get_peers(self, request_data):
        """Handle GET_PEERS request - return known peers"""
        peer_list = []
        
        for bucket in self.routing_table.values():
            peer_list.extend(bucket)
        
        # Limit to 50 peers
        peer_list = peer_list[:50]
        
        return {
            'type': 'PEERS',
            'peers': peer_list,
            'count': len(peer_list),
            'timestamp': time.time()
        }
    
    async def handle_request(self, reader, writer):
        """Handle incoming request"""
        try:
            # Read request
            data = await asyncio.wait_for(reader.read(8192), timeout=10)
            
            if not data:
                return
            
            request = json.loads(data.decode())
            request_type = request.get('type', 'UNKNOWN')
            
            self.stats['requests_handled'] += 1
            
            # Add requester to routing table if provided
            if 'node_id' in request:
                requester_host = writer.get_extra_info('peername')[0]
                self.add_peer(
                    request['node_id'],
                    requester_host,
                    request.get('port', 8000)
                )
            
            # Route request
            if request_type == 'PING':
                response = await self.handle_ping(request)
            elif request_type == 'FIND_NODE':
                response = await self.handle_find_node(request)
            elif request_type == 'STORE':
                response = await self.handle_store(request)
            elif request_type == 'FIND_VALUE':
                response = await self.handle_find_value(request)
            elif request_type == 'GET_PEERS':
                response = await self.handle_get_peers(request)
            else:
                response = {'type': 'ERROR', 'message': f'Unknown request type: {request_type}'}
            
            # Send response
            response_data = json.dumps(response).encode()
            writer.write(response_data)
            await writer.drain()
            
        except asyncio.TimeoutError:
            print(f"[TIMEOUT] Request timed out")
        except json.JSONDecodeError:
            print(f"[ERROR] Invalid JSON received")
        except Exception as e:
            print(f"[ERROR] {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def send_request(self, host, port, request):
        """Send request to another node"""
        try:
            # Add our info to request
            request['node_id'] = self.node_id
            request['port'] = self.port
            
            # Connect
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=5
            )
            
            # Send request
            request_data = json.dumps(request).encode()
            writer.write(request_data)
            await writer.drain()
            
            # Read response
            response_data = await asyncio.wait_for(reader.read(8192), timeout=10)
            response = json.loads(response_data.decode())
            
            writer.close()
            await writer.wait_closed()
            
            return response
            
        except Exception as e:
            print(f"[ERROR] Failed to contact {host}:{port} - {e}")
            return None
    
    async def bootstrap(self, bootstrap_nodes):
        """Bootstrap from known nodes"""
        print(f"\n[BOOTSTRAP] Starting bootstrap process...")
        print(f"[BOOTSTRAP] Contacting {len(bootstrap_nodes)} bootstrap nodes...")
        
        for host, port in bootstrap_nodes:
            print(f"[BOOTSTRAP] Trying {host}:{port}...")
            
            # Ping the node
            response = await self.send_request(host, port, {'type': 'PING'})
            
            if response and response.get('type') == 'PONG':
                print(f"[BOOTSTRAP] ✓ {host}:{port} responded")
                
                # Add to routing table
                self.add_peer(response['node_id'], host, port)
                
                # Get peers from this node
                peer_response = await self.send_request(host, port, {'type': 'GET_PEERS'})
                
                if peer_response and peer_response.get('type') == 'PEERS':
                    peers = peer_response.get('peers', [])
                    print(f"[BOOTSTRAP] Got {len(peers)} peers from {host}:{port}")
                    
                    # Add all peers to routing table
                    for peer in peers:
                        self.add_peer(peer['id'], peer['host'], peer['port'])
            else:
                print(f"[BOOTSTRAP] ✗ {host}:{port} no response")
        
        print(f"[BOOTSTRAP] Bootstrap complete. Known peers: {len(self.peers)}")
    
    async def periodic_stats(self):
        """Print statistics periodically"""
        while True:
            await asyncio.sleep(300)  # Every 5 minutes
            
            stats = self.get_stats()
            print(f"\n{'='*60}")
            print(f"[STATS] {datetime.now()}")
            print(f"  Node ID: {stats['node_id'][:16]}...")
            print(f"  Uptime: {stats['uptime_hours']} hours")
            print(f"  Known Peers: {stats['peers_known']}")
            print(f"  Items Stored: {stats['items_stored']}")
            print(f"  Requests Handled: {stats['total_requests']}")
            print(f"{'='*60}\n")
    
    async def run(self):
        """Run the DHT node"""
        # Start server
        server = await asyncio.start_server(
            self.handle_request,
            self.host,
            self.port
        )
        
        addr = server.sockets[0].getsockname()
        print(f"\n{'='*60}")
        print(f"DHT Node Started")
        print(f"  Node ID: {hex(self.node_id)[:20]}...")
        print(f"  Listening on: {addr[0]}:{addr[1]}")
        print(f"  Started at: {datetime.now()}")
        print(f"{'='*60}\n")
        
        # Start periodic stats
        asyncio.create_task(self.periodic_stats())
        
        async with server:
            await server.serve_forever()


async def main():
    """Main entry point"""
    
    # Bootstrap nodes (update these with real nodes later)
    # For now, this is just your node
    bootstrap_nodes = [
        # Add other bootstrap nodes here when available
        # ('bootstrap1.example.com', 8000),
        # ('bootstrap2.example.com', 8000),
    ]
    
    # Create node
    node = KademliaNode(host='0.0.0.0', port=8000)
    
    # Bootstrap if we have bootstrap nodes
    if bootstrap_nodes:
        await node.bootstrap(bootstrap_nodes)
    else:
        print("[INFO] No bootstrap nodes configured - running standalone")
        print("[INFO] Add bootstrap nodes to connect to network")
    
    # Run node
    await node.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Node stopped by user")
