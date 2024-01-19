#!/usr/bin/env python

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
root = None
for parent in file.parents:
    if parent.name == "ampscz-interview-qc":
        root = parent
sys.path.append(str(root))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass


import logging
from argparse import ArgumentParser

from rich.logging import RichHandler

from interviewqc.helpers import utils

MODULE_NAME = "interviewqc_move_to_new_root"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    config_params = utils.config(config_file, "general")
    data_root = Path(config_params["data_root"])
    logger.info(f"Data root: {data_root}")

    config_params = utils.config(config_file, "move")
    backup_root = Path(config_params["backup_root"])
    logger.info(f"Backup root: {backup_root}")

    arg_parser = ArgumentParser()
    arg_parser.add_argument(
        "--sites",
        dest="sites",
        nargs="+",
        required=True,
        help="The sites to move to the new root.",
    )

    args = arg_parser.parse_args()

    sites = args.sites
    logger.info(f"Sites: {sites}")
