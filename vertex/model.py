import itertools
import random
from typing import Tuple
from uuid import uuid4

from corvus.dto import Resources
from corvus.shared.alpha import RPC


class NodeInfo:
    def __init__(self, resources: Resources, address: Tuple[str, int]):
        self.resources = resources
        self.address = address
        self.uuid = str(uuid4())

        self.endpoints = {}


class EndpointInfo:
    def __init__(self, parent: NodeInfo, address: Tuple[str, int], name: str, resources: Resources):
        self.parent = parent
        self.resources = resources
        self.address = address
        self.uuid = str(uuid4())
        self.name = name


class VertexModel:

    def __init__(self):
        self._nodes = {}
        self._global_endpoints = {}

    def add_node(self, resources, addr) -> NodeInfo:
        node = NodeInfo(Resources(resources), addr)
        self._nodes[node.uuid] = node
        return node

    def add_endpoint(self, name, resources, addr, node_uuid) -> EndpointInfo:
        node = self._nodes[node_uuid] if node_uuid else None
        endpoint = EndpointInfo(node, addr, name, Resources(resources))

        if node:
            self._nodes[node_uuid].endpoints[name] = endpoint
        else:
            self._global_endpoints[name] = endpoint

        return endpoint

    def remove_node(self, uuid: str) -> NodeInfo:
        return self._nodes.pop(uuid)

    def run(self, rpc: RPC):
        next(iter(self._nodes)).start(rpc)

    def get_available_endpoint(self, name):
        all_endpoints = itertools.chain(*map(lambda n: list(n.endpoints.values()), self._nodes.values()))
        all_endpoints = itertools.chain(all_endpoints, self._global_endpoints.values())

        endpoints = list(filter(lambda e: e.name == name, all_endpoints))

        if not endpoints:
            return None

        # Random selection
        # TODO update to a better selection method
        return random.choice(endpoints)

    def info(self):
        return {
            "nodes": self._nodes
        }
