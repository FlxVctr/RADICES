import yaml
import os


class Config():

    config_path = "config.yml"
    config_template = "config_template.py"

    def __init__(self):
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.load(f)
        except FileNotFoundError:
            raise FileNotFoundError('''Could not find "config.yml".\n
            Please run "python3 make_config.py" or provide a config.yml''')

        self.mysql_config = self.config["mysql"]
        self.dbtype = self.mysql_config["dbtype"]
        self.dbhost = self.mysql_config["host"]
        self.dbuser = self.mysql_config["user"]
        self.dbpwd = self.mysql_config["passwd"]
        self.dbname = self.mysql_config["dbname"]

        
