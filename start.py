from setup import FileImport as FI
from configreader import Config
from make_db import DataBaseHandler

ckeys = FI.read_app_key_file()
seeds = FI.read_seed_file()
DBH = DataBaseHandler()
cfg = Config()

if cfg.dbtype == "sqlite":
    DBH.new_db(db_name=cfg.dbname)
# TODO: elif dbtype = sql / mysql etc
