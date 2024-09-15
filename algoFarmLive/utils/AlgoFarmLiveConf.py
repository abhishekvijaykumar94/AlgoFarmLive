import os

class AlgoFarmLiveConf:

    @staticmethod
    def get_value(key, file_name=None):
        PropertyManager.get_properties('AlgoFarmLive.properties')

class PropertyManager:
    @staticmethod
    def get_properties(file_name=None):
        if not file_name:
            file_name = 'default.properties'
        file_path = os.path.join(os.path.dirname(__file__), 'config', file_name)
        properties = {}
        with open(file_path, 'r') as f:
            for line in f:
                if '=' in line:
                    key, value = line.split('=', 1)
                    properties[key.strip()] = value.strip()
        return properties

    @staticmethod
    def get_value(key, file_name=None):
        properties = PropertyManager.get_properties(file_name)
        return properties.get(key)
