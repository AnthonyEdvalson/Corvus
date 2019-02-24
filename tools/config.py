import json
from typing import Tuple, Dict, Any, List

from corvus.tools.printing import tree


class Config:
    def __init__(self, json_path=None, data=None):
        self._data = None
        
        assert json_path is None or data is None  # Only json or data should be passed in
        
        if json_path is not None:
            self.load_json(json_path)
        
        if data is not None:
            self.load_dict(data)

    @staticmethod
    def merge(base: dict, d: dict) -> dict:
        """
            Merges a dictionary, similar to dict.update(), but is more friendly with nesting. {a: 1, b:{c: 2}} merged
            with {a: 2, b:{d: 5}} will give {a: 2, b:{c: 2, d: 5}}, whereas dict.update would give {a: 2, b:{d: 5}}

        :param base: The starting dictionary
        :param d: The dictionary they is applied "on top" of the base
        :return: The merged dictionary
        """
        merged = base.copy()

        for k in d:
            if k in base:
                d1 = isinstance(d[k], dict)
                d2 = isinstance(base[k], dict)

                if d1 and d2:
                    merged[k] = Config.merge(base[k], d[k])
                elif not d1 and not d2:
                    merged[k] = d[k]  # override existing value
                else:
                    raise Exception("Cannot Merge")  # If this error comes up often, give a nicer error message
            else:
                merged[k] = d[k]  # new key, add to base

        return merged

    def multi_select(self, keys: List[str]) -> 'Config':
        """
            Similar to select(), but multiple keys are given. Each key is selected sequentially, and the resulting
            configs are then merged together
            
        :param keys: The keys that will be merged in the resulting config
        :return: The merged config
        """
        
        new_data = {}
        
        for key in keys:
            new_data = Config.merge(new_data, self.select(key).as_dict())
        
        config = Config()
        config.load_dict(new_data)
        return config

    def load_json(self, path: str) -> 'Config':
        """
            Load Config data from a .json file

        :param path: the path of the file to load
        """
        if self._data:
            raise Exception("A Config can only load data once")

        assert not self._data
        data = json.load(open(path))

        return self.load_dict(data)

    def load_dict(self, data: dict) -> 'Config':
        self._data = data
        return self

    def get(self, key: str, default: Any = None) -> Any:
        """
            Get a value from the config, with an optional default if the key is not found

        :param key: The key of the data
        :param default: The value returned if the key does not exist
        :return: The value associated with the given key
        """
        d, key = self.resolve(key)

        if key in d:
            return d[key]
        else:
            return default

    def __getitem__(self, key: str) -> Any:
        d, key = self.resolve(key)

        if key not in d:
            raise self.not_found(key, d)

        return d[key]

    def adopt(self, key: str, config: 'Config') -> None:
        """
            Adopt a configuration under a certain key

        :param key: the key that the new config will be placed in
        :param config: the config that will be adopted
        """
        assert isinstance(config, Config)
        assert key not in self._data
        d, k = self.resolve(key)
        d[k] = config.as_dict()

    def alter(self, key: str, value: Any, exists: bool = True) -> None:
        """
            SHOULD NOT BE USED OUTSIDE OF TESTING AND DEBUGGING
            Changes a value in the Config

            Useful when tweaking a value for testing, but bad practice for production due to state issues, config
            generally should not be changed at runtime

        :param key: The key to tweak
        :param value: The value to set the key to
        :param exists: If the key should already exist in the config
        """
        d, k = self.resolve(key)

        if k not in d and exists:
            raise self.not_found(key, d, "If you want to create a new key when it doesnt exist, set exists to False")

        old_value = d[k] if k in d else "*undefined*"  # Only for print message
        d[k] = value

        # Alter should only be used in testing or debugging, so a warning is printed
        print("!!! Config Altered ({}: {} -> {})".format(k, old_value, value))

    def resolve(self, key) -> Tuple[any, str]:
        """
            Traverse through the Config using a "/" separated key

        :param key: The "/" separated key
        :return: A tuple, with the dictionary that contains the key, and the last part of the key.
        """
        assert self._data is not None

        keys = key.split("/")
        key = keys[-1]

        d = self._data
        for k in keys[:-1]:
            if k not in d:
                raise self.not_found(key, d, "Cannot traverse")
            d = d[k]
            if type(d) != dict:
                raise IndexError("'{}' is a {}, not a dictionary, cannot traverse to it".format(key, str(type(d))))

        return d, key

    @staticmethod
    def not_found(key: str, d: Dict[str, any], msg: str = "") -> IndexError:
        """
            Create a user friendly exception for a value not found in a dict

        :param key: The key that could not be found
        :param d: The dictionary the key could not be found in
        :param msg: Additional error information
        :return: The KeyError with a human friendly error message
        """
        s = tree("Config", d)

        return IndexError("'{}' not found in:\n{}\n  {}".format(key, s, msg))

    def select(self, key: str) -> 'Config':
        """
            Select a subconfig

        :param key: The name of the key for the subconfig
        :return: The subconfig
        """
        new_config = Config().load_dict(self[key])

        return new_config

    def save(self, path: str):
        """
            Save the current config to a file with the current mode

        :param path: The path to save the new config file in
        """
        with open(path, "w") as file:
            json.dump(self._data, file)

    def as_dict(self) -> dict:
        """
            Return the Config as a dictionary

        :return: the dictionary version of the Config
        """
        return self._data.copy()

    def __str__(self) -> str:
        return tree("Config", self._data)

    def __rep__(self) -> str:
        return str(self._data)

    def __eq__(self, other) -> bool:
        if type(other) is not Config:
            return False

        return self._data == other.as_dict()
