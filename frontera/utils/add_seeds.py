# -*- coding: utf-8 -*-
from frontera.core.manager import LocalFrontierManager
from frontera.settings import Settings
from frontera.logger.handlers import CONSOLE
from argparse import ArgumentParser
import logging
from logging.config import fileConfig
from os.path import exists


logger = logging.getLogger(__name__)

parser = ArgumentParser(description="Frontera local add seeds utility")
parser.add_argument('--config', type=str, required=True,
                    help='Settings module name, should be accessible by import')
parser.add_argument('--log-level', '-L', type=str, default='INFO',
                    help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")
parser.add_argument('--seeds-file', type=str, required=True, help="Seeds file path")
args = parser.parse_args()
settings = Settings(module=args.config)
logging_config_path = settings.get("LOGGING_CONFIG")
if logging_config_path and exists(logging_config_path):
    fileConfig(logging_config_path, disable_existing_loggers=False)
else:
    logging.basicConfig(level=args.log_level)
    logger.setLevel(args.log_level)
    logger.addHandler(CONSOLE)

fh = open(args.seeds_file, "rb")

logger.info("Starting local seeds addition from file %s", args.seeds_file)

manager = LocalFrontierManager.from_settings(settings)
manager.add_seeds(fh)
manager.stop()
manager.close()

logger.info("Seeds addition finished")