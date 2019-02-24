import json
from typing import List


class AppData:
    def __init__(self, name, command, cwd):
        self.name = name
        self.command = command
        self.cwd = cwd


# TODO make a config
class NodeConfig:
    def __init__(self, app_datas: List[AppData]):
        self.app_datas = app_datas

    @staticmethod
    def from_json_file(path: str):
        app_datas = []

        with open(path) as file:
            config_data = json.load(file)

        for app in config_data["apps"]:
            app_datas.append(AppData(app["name"], app["command"], app["cwd"]))

        return NodeConfig(app_datas)
