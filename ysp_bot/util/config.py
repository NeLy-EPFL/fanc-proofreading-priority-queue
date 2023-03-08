import yaml
from pathlib import Path

import ysp_bot


def load_config():
    config_path = Path(ysp_bot.__path__[0]) / 'config.yaml'
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return config

def load_credentials():
    config = load_config()
    with open(Path(config['credential_path']).expanduser()) as f:
        credentials = yaml.safe_load(f)
    return credentials