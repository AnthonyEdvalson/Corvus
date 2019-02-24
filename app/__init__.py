import os
import signal
from typing import Tuple
from uuid import UUID

from corvus.shared.endpoint import Endpoint, Task
from corvus.vertex.main import Vertex


class App(Endpoint):
    """
    An endpoint that is designed to run user code. It provides additional tools to make common endpoint actions simpler
    """

    def __init__(self, name: str):
        super().__init__(name, self.run_task_from_flow)
        self.add_task(Task(self._options, {}, "options"))
        self.node_uuid = None

    def task(self, name=None, **resources):
        """Decorator that adds the given function as a task on this App"""
        return lambda f: self.add_task(Task(f, resources, name))

    def start(self):
        vertex_addr, self.node_uuid = self._startup()

        """Runs the app locally"""
        signal.signal(signal.SIGTERM, self.stop)

        super().setup(vertex_addr)

        data = {
            "name": self.name,
            "resources": {},
            "host": self.address[0],
            "port": self.address[1],
            "node": self.node_uuid
        }
        self.vertex_send("vertex/connect_endpoint", data)

        super().start()

    def _startup(self) -> Tuple[Tuple[str, int], UUID]:
        node_uuid = os.environ.get("CORVUS_NODE_UUID", None)
        vert_str = os.environ.get("CORVUS_VERTEX_ADDR", None)

        if vert_str is None:
            # program ran without a vertex passed in, so the App will make a vertex to connect to
            vertex = Vertex(9000)
            vertex.start()
            vert_addr = vertex.address
            os.environ["CORVUS_VERTEX_ADDR"] = "{}:{}".format(*vert_addr)
        else:
            host, port = tuple(vert_str.split(":"))
            vert_addr = (host, int(port))

        # if node_uuid is None:
            # no parent node, so we're going to have to start up the other apps

        return vert_addr, node_uuid

    def get_address(self):
        """Returns the address of the App's server"""
        return self.server.get_address()

    def _options(self):
        """Return all App information in a human readable format. accessible by the 'options' task"""
        s = "\n\nApp '{}' is running on {}\n".format(self.name, "{0}:{1}".format(*self.server.get_address()))
        for task in self._tasks.values():
            s += "- {}\n".format(task.full_signature)

        return s
