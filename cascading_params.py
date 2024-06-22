import logging

import json5

LOGGER = logging.getLogger('cascading_params')
logging.basicConfig(level=logging.DEBUG)


class CascadingParams:
    def __init__(self, name):
        self.name = name
        self.param_list = []
        self.param_names = []

    def add_json5_to_start(self, name, file_path):
        params = self.load_json5_params(name, file_path)

        self.add_to_start(name, params)

    def add_to_start(self, name, params):
        self.param_names.insert(0, name)
        self.param_list.insert(0, params)

    def add_json5_to_end(self, name, file_path):
        params = self.load_json5_params(name, file_path)
        self.add_to_end(name, params)

    def add_to_end(self, name, params):
        self.param_names.append(name)
        self.param_list.append(params)

    def resolve(self):
        result_dict = {}

        for param in self.param_list:
            for key, value in param.items():
                if key in result_dict:
                    LOGGER.info(
                        f"Key {key} in dict was overridden. Old value: {result_dict[key]}. New value: {value}")
                result_dict[key] = value

        return result_dict

    @staticmethod
    def load_json5_params(name, file_path):
        if file_path:
            LOGGER.debug(f'Loading {name} from {file_path}')
            with open(file_path, 'r', encoding='utf-8') as fp:
                return json5.load(fp)
        LOGGER.info(f"Using empty parameter set for {name}")
        return json5.loads('{}')

    def __contains__(self, item):
        for param in reversed(self.param_list):
            if item in param:
                return True
        return False

    def __getitem__(self, item):
        for param in reversed(self.param_list):
            if item in param:
                return param[item]
        raise KeyError(item)

    def __getattr__(self, attr):
        try:
            return self.__getitem__(attr)
        except KeyError:
            raise AttributeError(f"{self.name} params object has no attribute {attr}")
