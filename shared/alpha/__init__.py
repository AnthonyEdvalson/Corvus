from typing import Union, List, Any

from corvus.shared.com import formatting


class ActionType:
    DELIM = "/"

    @staticmethod
    def from_str(string: str) -> 'ActionType':
        sections = string.split(ActionType.DELIM)
        endpoint = sections[0]
        task = sections[1:]

        return ActionType(endpoint, *task)

    @staticmethod
    def force_cast(action_type: Union['ActionType', str]):
        if type(action_type) is str:
            return ActionType.from_str(action_type)
        else:
            return action_type

    def __init__(self, endpoint: str, *task):
        self.endpoint = endpoint.lower()
        self.task = list(task)

    def to_list(self) -> List[str]:
        return [self.endpoint, *self.task]

    def get_task_str(self):
        return ActionType.DELIM.join(self.task)

    def __str__(self) -> str:
        return ActionType.DELIM.join(self.to_list())

    def __repr__(self) -> str:
        return self.__str__()


class Flow:
    """
    A Flow is all the data contained in a single transaction of data over the socket. Alpha's does not distinguish
    between requests and responses, so all data transferred over sockets in the Alpha protocol is considered a Flow.
    """

    FORM = "json"

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Flow':
        try:
            blocks = data.split(b"\n\n", 1)
            headers = blocks[0].decode().split("\n")

            action_type = ActionType.from_str(headers[0])
            status = headers[1]

            raw = blocks[1]

            content = formatting.deserialize(raw, "json")
            f = Flow(action_type, status, content)

            f.raw = raw
            return f
        except Exception as e:
            try:
                data = data.decode()
            except:
                pass

            raise Exception("ALPHA: Cannot parse {}".format(data)) from e

    def __init__(self, action_type: ActionType, status: str, content: Any=None):
        """

        :param action_type:
        :param status:
        :param content: Can be 'bytes' or an object. If bytes are given they will be stored as the message unmodified,
                        objects will be serialized to json, converted to byes, and stored as the message
        """
        self.action_type = action_type
        self.status = status

        if type(content) is bytes:
            self.raw = content
        else:
            self.raw = formatting.serialize(content, Flow.FORM)

        self.closed = False

    def get_content(self):
        return formatting.deserialize(self.raw, Flow.FORM)

    def to_bytes(self) -> bytes:
        msg = "{}\n".format(self.action_type)
        msg += "{}\n".format(self.status)

        msg += "\n"
        msg = msg.encode() + self.raw

        return msg

    def __str__(self) -> str:
        message = "{}: {}".format(self.status, str(self.raw))
        return formatting.shorten(message)

    def __repr__(self) -> str:
        return str(self.to_bytes())


class RPC:
    @staticmethod
    def from_request(req: Flow) -> 'RPC':
        return RPC(req.action_type, req.get_content())

    def __init__(self, path: Union[ActionType, dict], data: Any=None):
        # TODO clean / needed ?
        if type(path) is ActionType:
            self.path = path
        elif type(path) is dict:
            self.path = ActionType(**path)

        self.data = data

    def __str__(self) -> str:
        return "{} {}".format(self.path, self.data)
