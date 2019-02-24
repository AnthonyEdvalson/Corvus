import atexit
import time
import sys
from typing import List
from uuid import UUID

from corvus.shared.endpoint import BasicEndpoint, Task
from corvus.vertex.model import VertexModel


class Vertex(BasicEndpoint):
    def __init__(self, port: int=9000):
        super().__init__("vertex", self.run_task_from_flow, port)

        self.model = VertexModel()

        self.add_task(Task(self.connect_node))
        self.add_task(Task(self.disconnect))
        self.add_task(Task(self.start))
        self.add_task(Task(self.status))
        self.add_task(Task(self.lookup))
        self.add_task(Task(self.connect_endpoint))

    def connect_node(self, resources: dict, host: str, port: int):
        node = self.model.add_node(resources, (host, port))
        print(node.uuid)
        return node.uuid

    def connect_endpoint(self, name: str, resources: dict, host: str, port: int, node: UUID):
        self.model.add_endpoint(name, resources, (host, port), node)

    def disconnect(self, uuid: str):
        raise NotImplementedError()
        # self.model.remove_endpoint(uuid)

    def start(self):
        super().start()

    def status(self):
        return self.model.info()

    def lookup(self, endpoint_name):
        endpoint = self.model.get_available_endpoint(endpoint_name)

        if not endpoint:
            return None

        addr = endpoint.address
        return {"host": addr[0], "port": addr[1]}


if __name__ == '__main__':
    if len(sys.argv) > 2:
        vertex = Vertex(int(sys.argv[2]))
    else:
        vertex = Vertex()

    atexit.register(vertex.stop)
    vertex.start()
    while True:
        time.sleep(1)
