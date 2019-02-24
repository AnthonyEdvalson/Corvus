import inspect
import traceback
from typing import Callable, List, Dict


class ServerException(Exception):
    """
        Exception that is thrown when the connected server responds with the a status of error
    """
    def __init__(self, err_type: str, message: str, network_trace: List[Dict[str, str]]=None):
        self.err_type = err_type
        self.message = message
        self.network_trace = network_trace if network_trace is not None else []

        super().__init__(str(self))

    def push_network(self, path, info: str) -> None:
        """
            Push to the network stack

        :param path: The path for this jump in the network
        :param info: Additional information, such as routing info
        """
        self.network_trace.insert(0, {"path": str(path), "info": info})

    def __str__(self) -> str:
        s = "\n"

        s += "NetworkTrace\n"
        for nt in self.network_trace:
            s += "  " + nt["path"]
            s += "\n"
            s += "    " + nt["info"].replace("\n", "\n    ")
            s += "\n"

        s += "\n"
        s += self.message

        return s


class RoutedException(Exception):
    """
        Exception that is thrown when an exception occurs within a routed function call
    """
    def __init__(self, func_name, func: Callable, e: Exception):
        super().__init__()
        self.sig = func_name + str(inspect.signature(func))
        self.e = e

    def __str__(self) -> str:
        return ''.join(traceback.format_exception(etype=type(self.e), value=self.e, tb=self.e.__traceback__))
