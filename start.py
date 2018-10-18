from setup import Config
from database_handler import DataBaseHandler
from collector import Connection

tw_conn = Connection()
print("Twitter Connection established successfully")

DBH = DataBaseHandler()
cfg = Config()

if cfg.dbtype == "sqlite":
    DBH.new_db(db_name=cfg.dbname)
# TODO: elif dbtype = sql / mysql etc
