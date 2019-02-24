from typing import Tuple


class RemoteException(Exception):
    """
        Exception that is thrown when the connected server responds with the a status of error
    """
    def __init__(self, err_type: str, message: str, network_trace: Tuple[str, str]=None):
        self.err_type = err_type
        self.message = message
        self.network_trace = network_trace if network_trace is not None else []

        super().__init__(str(self))

    def push_network(self, location, data: str) -> None:
        """
        Push to the network stack, the network stack is used to see the series of ports a message went through

        :param location: An identifier for current device
        :param data: the data passed into the current device
        """
        self.network_trace.insert(0, (str(location), data))

    def __str__(self) -> str:
        s = "\n\n"

        s += "Network Trace (most recent message last):\n"
        for nt in self.network_trace:
            s += "  " + nt[0]
            s += "\n"
            s += "    " + nt[1].replace("\n", "\n    ")
            s += "\n"

        s += "\nRemote Execption Message:\n"
        s += self.message

        return s
