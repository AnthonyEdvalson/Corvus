import atexit
import os
import subprocess
import sys
import socket
import time
from enum import Enum

from corvus.node.config import NodeConfig
from corvus.shared.endpoint import Endpoint


class AppProcess:
    class AppState(Enum):
        STOPPED = 0
        RUNNING = 1
        STOPPING = 2

    def __init__(self, node, app_data):
        self.node = node
        self.process = None
        self.name = app_data.name
        self.app_data = app_data
        self.cwd = app_data.cwd

    def start(self):
        env = os.environ
        env.update({
            "CORVUS_NODE_UUID": self.node.uid,
            "CORVUS_VERTEX_ADDR": "{}:{}".format(*self.node.vert_addr)
        })

        self.process = subprocess.Popen([self.app_data.command], shell=True, env=env, cwd=self.cwd)

    def stop(self) -> int:
        self.process.terminate()

        try:
            self.process.wait(10)
        except subprocess.TimeoutExpired:
            self.process.kill()

        code = self.process.returncode
        self.process = None

        return code


class Node(Endpoint):
    def __init__(self, config: NodeConfig, vert_addr):
        self.config = config
        self.vert_addr = vert_addr
        self.uid = None

        self.apps = []

        for app_data in self.config.app_datas:
            self.apps.append(AppProcess(self, app_data))

        hostname = socket.gethostname()
        super().__init__(hostname, self._handler)

    def _handler(self, flow):
        pass

    def start(self):
        super().setup(self.vert_addr)

        data = {
            "resources": {},
            "host": self.address[0],
            "port": self.address[1],
        }
        self.uid = self.vertex_send("vertex/connect_node", data)

        super().start()

        for app in self.apps:
            app.start()

        while True:
            time.sleep(1)


def main():
    path = sys.argv[1]
    c = NodeConfig.from_json_file(path)

    host, port = sys.argv[2].split(":")
    vert_addr = (host, int(port))

    n = Node(c, vert_addr)
    atexit.register(n.stop)
    n.start()


if __name__ == '__main__':
    main()
