import inspect
import json
from typing import Callable, Any


def tree(top: str, d: dict) -> str:
    s = top

    # put all non-branching before branching
    items = list(d.items())
    sorted_items = list(filter(lambda x: not isinstance(x[1], dict), items))
    sorted_items.extend(filter(lambda x: isinstance(x[1], dict), items))

    for i in range(0, len(sorted_items)):
        item = sorted_items[i]
        last = i == len(sorted_items) - 1
        branching = isinstance(item[1], dict)

        s += "\n├── " if not last else "\n└── "

        if branching:
            ss = tree(*item)
            ss = ss.replace("\n", "\n│   " if not last else "\n    ")
            s += ss
        else:
            if callable(item[1]):
                s += "{}{}".format(item[0], inspect.signature(item[1]))
            else:
                s += "{}: {}".format(*item)

    if len(sorted_items) == 0:
        s += "\n└── <empty>"

    return s


def obj_to_str(obj: Any) -> str:
    return json.dumps(obj, indent=2)


def signature(func: Callable) -> str:
    return func.__name__ + str(inspect.signature(func))
