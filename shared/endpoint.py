import inspect
import traceback
from abc import ABC
from inspect import Parameter
from typing import Callable, Any, Tuple, Union

import parseltongue
from parseltongue import ClientConnection
from corvus.shared.alpha import Flow, ActionType
from corvus.shared.alpha.errors import RemoteException
from corvus.shared.logging import log_debug
from corvus.tools.printing import signature


class EndpointServer:

    def __init__(self, endpoint: 'BasicEndpoint', handler: Callable[[Flow], Any], port: int=0):
        sig = inspect.signature(handler)
        if len(sig.parameters) != 1:
            raise TypeError("EndpointServer Handler '{}' must have 2 arguments. "
                            "First is self, the second is to accept a Flow object".format(handler))

        self.handler = handler
        self.endpoint = endpoint
        self.server = parseltongue.Server(self.handle_binary, port=port)

    def open(self):
        self.server.open()

    def handle_binary(self, data: bytes):
        request = Flow.from_bytes(data)

        try:
            response_data = self.handler(request)
            response = Flow(request.action_type, "OKAY", response_data)

        except Exception as e:
            se = RemoteException(type(e).__name__, traceback.format_exc())
            se.push_network(self.endpoint.name, str(request.get_content()))
            response = Flow(request.action_type, "ERROR", se)

        return response.to_bytes()

    def close(self):
        self.server.close()

    def get_address(self) -> Tuple[str, int]:
        return self.server.address


class EndpointClient:
    def __init__(self):
        self.client = parseltongue.Client()

    def connect(self, addr: Tuple[str, int]):
        con = self.client.connect(addr)
        return EndpointClientConnection(con)

    def close(self):
        self.client.close()


class EndpointClientConnection:
    """
        Wrapper around ClientConnection that can send and receive data using the Alpha protocol instead of raw bytes
    """

    def __init__(self, connection: ClientConnection):
        self.connection = connection

    def send(self, action_type: Union[str, ActionType], data):
        action_type = ActionType.force_cast(action_type)

        request = Flow(action_type, "ASK", data)
        log_debug("CALL    {}({})".format(action_type.get_task_str(), request.get_content()))
        request_bytes = request.to_bytes()

        response_bytes = self.connection.send(request_bytes)

        response = Flow.from_bytes(response_bytes)
        content = response.get_content()

        log_debug("RECV    {}({}) -> {}".format(action_type.get_task_str(), data, content))

        if response.status == "ERROR":
            se = RemoteException(**content)
            print(str(se))
            raise se

        return content


class Task:
    """
    Represents a single task in an endpoint.
    """

    def __init__(self, function: Callable, resources: dict=None, name: str=None):
        self.name = name if name is not None else function.__name__

        self._function = function
        self.resources = resources if resources is not None else {}

        self._using_kwargs = False

        params = inspect.signature(self._function).parameters.values()
        self._required_args = set()

        for param in params:
            kind = param.kind

            if kind == Parameter.VAR_KEYWORD:
                self._using_kwargs = True
            elif kind == Parameter.POSITIONAL_OR_KEYWORD:
                self._required_args.add(param.name)
            else:
                message = "Corvus only supports standard arguments or **kwargs in {}"
                raise NotImplementedError(message.format(function.__name__))

        self.signature = signature(self._function)
        self.full_signature = "{}  # {}".format(self.signature, self._function.__doc__)

    def run(self, kwargs):
        if not self.valid_args(kwargs):
            string = "Invalid arguments for {}\n\nexpected {}\nreceived {}"
            received_sig = self.name + "({})".format(",".join(list(kwargs.keys())))
            raise Exception(string.format(self.name, self.signature, received_sig))
        return self._function(**kwargs)

    def valid_args(self, kwargs):

        # if kwargs is not a dict, it cannot be passed as kwargs
        if type(kwargs) is not dict:
            return False

        # if not all required args are met, these kwargs are not valid
        if self._required_args > set(kwargs):
            return False

        # if there are extra args given, and kwargs isn't used, these kwargs are not valid
        if self._required_args < set(kwargs) and not self._using_kwargs:
            return False

        return True


class BasicEndpoint(ABC):
    """
    Simple Endpoint that contains a server, client, and tasks.
    """
    def __init__(self, name: str, server_handler: Callable, port: int=0):
        self.name = name
        self.server = EndpointServer(self, server_handler, port)
        self.client = EndpointClient()
        self.address = None
        self._tasks = {}

    def add_task(self, task: Task):
        self._tasks[task.name] = task

    def start(self, **kwargs):
        self.server.open()
        self.address = self.server.get_address()
        print("{}:{}".format(*self.address), flush=True)

    def stop(self):
        self.server.close()
        self.client.close()

    def run_task_from_flow(self, flow: Flow):
        return self.run_task(flow.action_type.get_task_str(), **flow.get_content())

    def run_task(self, task_name: str, **content):
        log_debug("INVO    {}({})".format(task_name, content))

        if task_name not in self._tasks:
            raise TaskNotFoundException(type(self), task_name, list(self._tasks.keys()))

        task = self._tasks[task_name]
        res = task.run(content)

        log_debug("REPL    {}({}) -> {}".format(task_name, content, res))

        return res

    def options(self):
        """Return all App information in a human readable format"""

        addr = "{0}:{1}".format(*self.server.get_address())
        s = "\n\n'{} {}' is running on {}\n".format(type(self).__name__, self.name, addr)
        for task in self._tasks.values():
            s += "- {}\n".format(task.full_signature)

        return s


class Endpoint(BasicEndpoint):
    """
    Generic Endpoint, has all basic features including connection management, and endpoint resolution. This class can
    be inherited by any Endpoint that uses a connection to a Vertex.
    """

    def __init__(self, name: str, server_handler: Callable):
        super().__init__(name, server_handler)
        self.vertex = None
        self.connections = {}

    def connect(self, endpoint_name: str):
        self.connections[endpoint_name] = None

    def send(self, action_type: Union[str, ActionType], data) -> Any:
        action_type = ActionType.force_cast(action_type)

        endpoint = action_type.endpoint

        if endpoint not in self.connections:
            raise Exception("Unknown connection to '{}'".format(endpoint))

        connection = self.connections[endpoint]

        if connection is None:
            message = "Connection to '{}' has not been created, have you run start() on {}?"
            raise Exception(message.format(endpoint, self.name))

        return connection.send(action_type, data)

    def setup(self, vertex_addr):
        super().start()
        self.vertex = self.client.connect(vertex_addr)

    def start(self):
        for endpoint_name in self.connections.keys():
            response = self.vertex.send(ActionType("vertex", "lookup"), {"endpoint_name": endpoint_name})

            if not response:
                raise NoEndpointError(endpoint_name)

            host = response["host"]
            port = response["port"]
            endpoint_addr = (host, port)

            connection = self.client.connect(endpoint_addr)
            self.connections[endpoint_name] = connection

    def vertex_send(self, action_type: Union[str, ActionType], data):
        return self.vertex.send(action_type, data)


class NoEndpointError(Exception):
    def __init__(self, endpoint_name: str):
        super().__init__("There are no '{}' endpoints in the network to connect to".format(endpoint_name))


class ResolutionError(Exception):
    @staticmethod
    def not_found_message(type, value_type, key, available_keys):
        message = "\n\nThe {0} '{1}' could not be found in {3}.\nValid keys are: {2}"
        return message.format(value_type, key, ", ".join(available_keys), type)


class TaskNotFoundException(ResolutionError):
    def __init__(self, type, task_name, available_task_names):
        message = self.not_found_message(type.__name__, "task", task_name, available_task_names)
        super().__init__(message)
