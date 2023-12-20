#!/usr/bin/env python

import sys
from pathlib import Path
from typing import List

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
import json

from rich.logging import RichHandler

from interviewqc.helpers import utils, db
from interviewqc.helpers.config import config
from interviewqc.models.site import Site

MODULE_NAME = "interviewqc_import_sites"

console = utils.get_console()

MODULE_NAME = "interviewqc_init_psql"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def import_json(config_file: Path, sites_json: Path):
    with open(sites_json) as f:
        sites_j = json.load(f)

    sites: List[Site] = []

    for site in sites_j:
        site_id = site["id"]
        site_name = site["name"]
        country = site["country"]
        network = site["network"]

        site_m = Site(site_id, site_name, country, network)
        sites.append(site_m)

    sql_queries: List[str] = []
    for site in sites:
        query = site.to_sql()
        sql_queries.append(query)

    db.execute_queries(config_file=config_file, queries=sql_queries)


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    config_params = config(config_file, "general")
    sites_json = Path(config_params["sites_json"])
    logger.debug(f"Using sites json file: {sites_json}")

    logger.info("Importing sites json file to database")
    import_json(config_file=config_file, sites_json=sites_json)

    logger.info("Done")
