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
from typing import List, Tuple
import os
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor

from rich.logging import RichHandler

from interviewqc.helpers import utils, db
from interviewqc.models.interview_raw import InterviewRaw
from interviewqc.models.file import File

MODULE_NAME = "interviewqc_import_interview_files"

# Parallel processing settings
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


def get_all_interview_paths(config_file: Path) -> List[Tuple[Path, str]]:
    """
    Retrieves a list of interview paths that have not been imported yet.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        List[Tuple[Path, str]]: A list of interview paths that have not been imported yet.
    """
    sql_query = """
    SELECT interview_path, interview_name
    FROM interviews
    WHERE interview_path NOT IN (
        SELECT interview_path FROM interview_raw
    );
    """

    df = db.execute_sql(config_file=config_file, query=sql_query)

    interview_paths = df["interview_path"].tolist()
    interview_names = df["interview_name"].tolist()

    # cast str to Path
    interview_paths = [Path(interview_path) for interview_path in interview_paths]

    interview_paths_with_name: List[Tuple[Path, str]] = list(
        zip(interview_paths, interview_names)
    )

    return interview_paths_with_name


def scan_all_files_for_interview(interview_path: Path) -> List[File]:
    """
    Scans all files in the given interview_path directory and returns a list of File objects.

    Args:
        interview_path (Path): The path to the directory containing the interview files.

    Returns:
        List[File]: A list of File objects representing the interview files found in the directory.
    """
    interview_files: List[File] = []

    if interview_path.is_file():
        interview_file = File.from_path(interview_path)
        interview_files.append(interview_file)
        return interview_files

    for root, dirs, files in os.walk(interview_path):
        for file in files:
            file_path = Path(root) / file
            if file.startswith(".checksum"):  # ignore checksum files
                continue
            interview_file = File.from_path(file_path)

            interview_files.append(interview_file)

    return interview_files


def process_interview_path(
    interview_path_with_name: Tuple[Path, str], config_file: Path
):
    """
    Processes a single interview path in a separate process.

    Args:
        interview_path (Tuple[Path, str]): A tuple containing the interview path and the interview name.
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
    interview_path, interview_name = interview_path_with_name

    files = scan_all_files_for_interview(interview_path=interview_path)
    sql_queries = [interview_file.to_sql() for interview_file in files]

    for interview_file in files:
        interview_mapping = InterviewRaw(
            interview_name=interview_name, file_path=interview_file.file_path
        )
        sql_queries.append(interview_mapping.to_sql())

    db.execute_queries(
        config_file=config_file,
        queries=sql_queries,
        show_commands=False,
        silent=True,
    )


def wrapper_process_interview_path(args):
    """
    Wrapper function to process interview path.

    Args:
        args: A tuple of arguments to be passed to the process_interview_path function.

    Returns:
        The result of the process_interview_path function.
    """
    return process_interview_path(*args)


def scan_for_interview_files(config_file: Path):
    """
    Scans for interview files and processes them.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
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
                    executor.submit(
                        wrapper_process_interview_path, (path_with_name, config_file)
                    )
                    for path_with_name in interview_paths
                ]

                for future in concurrent.futures.as_completed(futures):
                    progress.update(task, advance=1)

        # with multiprocessing.Pool() as pool:
        #     pool.starmap(process_interview_path, [(path, config_file) for path in interview_paths])

        else:
            for interview_path_with_name in interview_paths:
                progress.update(task, advance=1)
                files: List[File] = []

                interview_path, interview_name = interview_path_with_name

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
                        interview_name=interview_name,
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
