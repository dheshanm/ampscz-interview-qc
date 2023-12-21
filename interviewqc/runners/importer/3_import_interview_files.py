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
from typing import List
from datetime import datetime
import os
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor

from rich.logging import RichHandler

from interviewqc.helpers import utils, db
from interviewqc.helpers.config import config
from interviewqc.models.interview_raw import InterviewRaw
from interviewqc.models.file import File

MODULE_NAME = "interviewqc_import_interview_files"
PARALLEL = True
NUM_WORKERS = 4

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_all_interview_paths(config_file: Path) -> List[Path]:
    sql_query = """
    SELECT interview_path FROM interviews WHERE interview_path NOT IN (
        SELECT interview_path FROM interview_raw
    );
    """

    df = db.execute_sql(config_file=config_file, query=sql_query)

    interview_paths = df["interview_path"].tolist()

    # cast str to Path
    interview_paths = [Path(interview_path) for interview_path in interview_paths]

    return interview_paths


def scan_all_files_for_interview(interview_path: Path) -> List[File]:
    interview_files: List[File] = []

    for root, dirs, files in os.walk(interview_path):
        for file in files:
            file_path = Path(root) / file
            file_name = file_path.name
            file_type = file_path.suffix
            file_size = file_path.stat().st_size
            file_size_mb = file_size / 1024 / 1024

            m_time = datetime.fromtimestamp(file_path.stat().st_mtime)

            interview_file = File(
                file_name=file_name,
                file_type=file_type,
                file_size=file_size_mb,
                file_path=file_path,
                m_time=m_time,
            )

            interview_files.append(interview_file)

    return interview_files


def process_interview_path(interview_path: Path, config_file: Path):
    """Processes a single interview path in a separate process."""

    files = scan_all_files_for_interview(interview_path=interview_path)
    sql_queries = [interview_file.to_sql() for interview_file in files]

    for interview_file in files:
        interview_mapping = InterviewRaw(
            interview_path=interview_path, file_path=interview_file.file_path
        )
        sql_queries.append(interview_mapping.to_sql())

    db.execute_queries(
        config_file=config_file,
        queries=sql_queries,
        show_commands=False,
        silent=True,
    )


def wrapper_process_interview_path(args):
    return process_interview_path(*args)


def scan_for_interview_files(config_file: Path):
    global PARALLEL
    global NUM_WORKERS

    interview_paths = get_all_interview_paths(config_file=config_file)

    with utils.get_progress_bar() as progress:
        task = progress.add_task(
            "Scanning for interview files", total=len(interview_paths)
        )

        if PARALLEL:
            with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
                futures = [
                    executor.submit(wrapper_process_interview_path, (path, config_file))
                    for path in interview_paths
                ]

                for future in concurrent.futures.as_completed(futures):
                    progress.update(task, advance=1)

        # with multiprocessing.Pool() as pool:
        #     pool.starmap(process_interview_path, [(path, config_file) for path in interview_paths])

        else:
            for interview_path in interview_paths:
                progress.update(task, advance=1)
                files: List[File] = []

                interview_files = scan_all_files_for_interview(
                    interview_path=interview_path
                )
                files.extend(interview_files)

                sql_queries: List[str] = []
                for interview_file in files:
                    query = interview_file.to_sql()
                    sql_queries.append(query)

                for interview_file in interview_files:
                    interview_mapping = InterviewRaw(
                        interview_path=interview_path,
                        file_path=interview_file.file_path,
                    )

                    sql_queries.append(interview_mapping.to_sql())

                db.execute_queries(
                    config_file=config_file,
                    queries=sql_queries,
                    show_commands=False,
                    silent=True,
                )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    logger.info("Getting all interview files")
    scan_for_interview_files(config_file=config_file)

    logger.info("Done")
