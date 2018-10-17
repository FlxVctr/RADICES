import yaml


class Config():

    config_path = "config.yml"
    config_template = "config_template.py"

    def __init__(self):
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.load(f)
        except FileNotFoundError:
            raise FileNotFoundError('''Could not find "config.yml".\n
            Please run "python3 make_config.py" or provide a config.yml''')
