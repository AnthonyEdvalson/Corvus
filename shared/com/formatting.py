import datetime
import json
from enum import Enum
from typing import Any
from uuid import UUID


def shorten(msg: str, max_len: int=100) -> str:
    """
        Shorten this string, if longer than max_len, truncate and add "..."

    :param msg: the message to shorten
    :param max_len: the maximum length for this string
    :return: the shortened string
    """
    msg = str(msg)
    return msg if len(msg) < max_len else msg[:max_len] + '...'


def serialize(data: Any, form: str) -> bytes:
    """
        Serialize the given data to the given format

    :param data: data to serialize
    :param form: format to serialize data to
    :return: serialized data (as bytes)
    """
    assert form in forms
    return forms[form].serialize(data)


def deserialize(data: bytes, form: str) -> Any:
    """
        Deserialize the given bytes with the given format

    :param data: the bytes to deserialize
    :param form: the format the bytes are in
    :return: the deserialized data
    """
    assert type(data) is bytes
    return forms[form].deserialize(data)


class LooseJsonEncoder(json.JSONEncoder):
    """
        A flexible JSON encoder that can parse times, configs, enums, and cassandra models. Unknown data will
        have its attributes extracted and saved as a dict
    """

    def default(self, o: Any):
        """
            Parse data

        :param o: the data to parse
        :return: reformatted data
        """

        if isinstance(o, Enum):
            return o.value

        t = type(o)

        types = {
            datetime.timedelta: lambda td: td.total_seconds(),
            datetime.datetime: lambda dt: dt.replace(tzinfo=datetime.timezone.utc).timestamp(),
            bytes: lambda b: b.decode(),
            UUID: lambda u: str(u)
        }

        if t in types:
            return types[t](o)

        return dict(filter(lambda a: a[0][0] != "_", o.__dict__.items()))


class Json:
    """
        Class for Json format
    """

    def __init__(self) -> None:
        self.encoder = LooseJsonEncoder()

    def serialize(self, data: Any) -> bytes:
        """
            Convert object to bytes

        :param data: object to parse
        :return: byte version of object
        """
        return self.encoder.encode(data).encode()

    @staticmethod
    def deserialize(data: bytes) -> Any:
        """
            Convert bytes to object

        :param data: bytes to parse
        :return: object version of object
        """
        return json.loads(data.decode())


class Text:
    """
        Class for text format
    """

    @staticmethod
    def serialize(data: Any) -> bytes:
        """
            Convert string to bytes

        :param data: string to parse
        :return: byte version of string
        """
        return str(data).encode()

    @staticmethod
    def deserialize(data: bytes) -> str:
        """
            Convert bytes to string

        :param data: the bytes to parse
        :return: string version of bytes
        """
        return data.decode()


class Bytes:
    """
        Class for bytes format
    """

    @staticmethod
    def serialize(data: bytes) -> bytes:
        """
            Convert byte to bytes, so do nothing

        :param data: the data to do nothing with
        :return: exactly what you gave it
        """
        assert type(data) is bytes
        if data is None:
            return b''

        return data

    @staticmethod
    def deserialize(data: bytes) -> bytes:
        """
            Convert bytes to bytes, so don't do anything

        :param data: the data to do nothing with
        :return: exactly what you gave it
        """
        return data


forms = {
    "json": Json(),
    "text": Text(),
    "bytes": Bytes()
}
