import configparser
import os

curr_path = os.path.dirname(os.path.abspath(__file__))

class PropertyManager:
    _config = configparser.ConfigParser()
    current_dir = os.path.dirname(os.getcwd())
    properties_path = os.path.join(current_dir, 'resources', 'AlgoFarmLive.properties')
    _config.read(properties_path)

    @staticmethod
    def getValue(key):
        """Retrieve a value based on the given key."""
        try:
            # Assuming the properties are under a default section named 'DEFAULT'
            return PropertyManager._config['DEFAULT'][key]
        except KeyError:
            print(f"Key '{key}' not found in properties file.")
            return None



# Example usage:
# value = PropertyManager.getValue('DESTINATION_PATH')
# print(value)

# Note: Adjust the path to 'QuantMechanic.properties' in the _config.read(...) line to the actual path before using.
