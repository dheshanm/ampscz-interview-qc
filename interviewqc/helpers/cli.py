"""
Provides helper functions for command line operations.
"""

import subprocess
from logging import Logger
from pathlib import Path
from typing import List


def get_repo_root() -> Path:
    """
    Returns the root directory of the current Git repository.

    Uses the command `git rev-parse --show-toplevel` to get the root directory.
    """
    repo_root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
    repo_root = repo_root.decode("utf-8").strip()
    return Path(repo_root)


def remove_files(files: List[Path], base_dir: Path, logger: Logger) -> None:
    """
    Remove a list of files and remove any empty directories.

    Args:
        files (List[Path]): A list of paths to files to be removed.
        base_dir (Path): The base directory to remove empty directories from.
        logger (Logger): The logger to use.
    """
    for file in files:
        if file.exists():
            logger.info(f"Removing {file}")
            file.unlink()

            path = file.parent
            while path != base_dir and path != Path("/"):
                if not list(path.iterdir()):
                    path.rmdir()
                    path = path.parent
                else:
                    break
        else:
            logger.warning(f"Expected File, {file} does not exist.")
