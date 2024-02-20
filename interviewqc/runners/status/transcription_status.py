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
from typing import List, Dict, Set, Tuple

from rich.logging import RichHandler
import pandas as pd

from interviewqc.helpers import cli, utils, db, dpdash
from interviewqc.models.transcription_status import TranscriptionStatus

MODULE_NAME = "interviewqc.runners.status.transcription_status"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def fix_day_to_session_map(
    day_to_session_map: Dict[int, int]
) -> Dict[int, int]:
    """
    Sometimes the pipeline misses the session and assigns the same session number
    in that case reassign the session number.

    Args:
        day_to_session_map (Dict[int, int]): A dictionary mapping day number to session number,
            with potentially duplicate session numbers.

    Returns:
        Dict[int, int]: A dictionary mapping day number to session number.
    """

    if len(day_to_session_map) == 1:
        return day_to_session_map

    # check if there are any duplicate session numbers
    session_numbers: Set = set()
    for session in day_to_session_map.values():
        if session in session_numbers:
            break
        session_numbers.add(session)
    else:
        return day_to_session_map
    
    # if there are duplicate session numbers, reassign the session numbers
    # based on the day number
    new_day_to_session_map = {}
    days = sorted(day_to_session_map.keys())

    for i, day in enumerate(days):
        new_day_to_session_map[day] = i + 1

    return new_day_to_session_map

def get_day_and_session_from_filename(
        filename: str
) -> Tuple[int, int]:
    """
    Get the day and session number from the filename in the format:
        site_subject_interviewAudioTranscript_<type>_day0001_session001.wav

    Args:
        filename (str): The filename of the interview.

    Returns:
        Tuple[int, int]: A tuple of the day and session number.
    """

    session = filename.split("_")[-1].split(".")[0]
    session_number = int(session.split("session")[1])

    day = filename.split("_")[-2]
    day_number = int(day.split("day")[1])

    return day_number, session_number


def get_day_session_map(audio_dir: Path) -> Dict[int, int]:
    """
    Get a map of day to session number from the audio files in the directory.

    Args:
        audio_dir (Path): The directory containing the audio files.

    Returns:
        Dict[int, int]: A map of day to session number.
    """
    day_to_session_map: Dict[int, int] = {}
    for audio in audio_dir.glob("*.wav"):
        # site_subject_interviewAudioTranscript_<type>_day0001_session001.wav
        file_name = audio.name
        day, session = get_day_and_session_from_filename(file_name)
        day_to_session_map[day] = session

    return fix_day_to_session_map(day_to_session_map)


def explore_subject_status(
    subject_interview_dir: Path
) -> Dict[int, Tuple[int, str]]:
    """
    Returns a Map of Day to Session number, Status

    Args:
        subject_interview_dir (Path): The directory containing the subject's interviews.

    Returns:
        Dict[int, Tuple[int, str]]: A map of day to session number and status.
            e.x. {1: (1, "pending"), 2: (2, "completed")}
    """
    session_status_map: Dict[int, str] = {}
    day_status_map: Dict[int, str] = {}

    status_dir_map: Dict[str, Path] = {
        "pending": subject_interview_dir / "pending_audio",
        "rejected": subject_interview_dir / "rejected_audio",
        "completed": subject_interview_dir / "completed_audio",
    }

    day_to_session_maps: Dict[str, Dict[int, int]] = {}
    for status, dir in status_dir_map.items():
        if not dir.exists():
            continue

        day_to_session_map = get_day_session_map(dir)
        day_to_session_maps[status] = day_to_session_map

        for day, session in day_to_session_map.items():
            if session in session_status_map:
                logger.warning(
                    f"{subject_interview_dir}: Session {session} already exists in the map as {session_status_map[session]}"
                )
            session_status_map[session] = status

        for day, session in day_to_session_map.items():
            if day in day_status_map:
                logger.warning(
                    f"{subject_interview_dir}: Day {day} already exists in the map as {day_status_map[day]}"
                )
            day_status_map[day] = status


    day_to_session_status_map: Dict[int, Tuple[int, str]] = {}
    for day, status in day_status_map.items():
        session = day_to_session_maps[status][day]
        day_to_session_status_map[day] = (session, status)

    return day_to_session_status_map


def get_pipeline_status_df(
    data_root: Path,
    network: str
) -> pd.DataFrame:
    """
    Get the pipeline status of the interviews.

    Args:
        data_root (Path): The root directory of the data.
        network (str): The network name.

    Returns:
        pd.DataFrame: A DataFrame containing the pipeline status of the interviews.
    """
    df = pd.DataFrame()
    protected_dir = data_root / "PROTECTED"

    studies = protected_dir.iterdir()
    studies = [s.name for s in studies if s.is_dir() and s.name.startswith(network)]

    def add_data_to_df(
        subject: str,
        study: str,
        interview_type: str,
        status_map: Dict[int, Tuple[int, str]],
        df: pd.DataFrame
    ) -> pd.DataFrame:
        data = []
        for day, session_status in status_map.items():
            interview_name = dpdash.get_dpdash_name(
                study=study,
                subject=subject,
                data_type="interview",
                category=interview_type,
                time_range=f"day{day:04d}"
            )
            session, status = session_status
            data.append({
                "subject_id": subject,
                "study_id": study,
                "interview_type": interview_type,
                "interview_name": interview_name,
                "day": day,
                "session": session,
                "pipeline_status": status
            })
        df = pd.concat([df, pd.DataFrame(data)])

        return df

    for study in studies:
        study_dir = protected_dir / study
        interviews_dir = study_dir / "processed"
        if not interviews_dir.exists():
            continue

        subjects = interviews_dir.iterdir()
        subjects = [s.name for s in subjects if s.is_dir()]

        for subject in subjects:
            subject_dir = interviews_dir / subject
            interview_dir = subject_dir / "interviews"

            for interview_type in ["open", "psychs"]:
                interview_type_dir = interview_dir / interview_type
                if not interview_type_dir.exists():
                    continue

                status_dict = explore_subject_status(interview_type_dir)
                df = add_data_to_df(
                    subject=subject,
                    study=study,
                    interview_type=interview_type,
                    status_map=status_dict,
                    df=df
                )

    return df

def check_transcript_status(
    subject: str,
    study: str,
    interview_type: str,
    day: int,
    data_dir: Path
) -> str:
    """
    Get the status of the transcript file.

    Args:
        subject (str): The subject ID.
        study (str): The study ID.
        interview_type (str): The interview type.
        day (int): The day number.
        data_dir (Path): The root directory of the data.

    Returns:
        str: The status of the transcript file.
            Can be "exists", "prescreening", or "missing".
    """
    transcript_dir = data_dir / "PROTECTED" / study / "processed" / subject / "interviews" / interview_type / "transcripts"

    # Transcript name template:
    # study_subject_interviewAudioTranscript_<interview_type>_day0001_session001.txt
    transcript_name = f"{study}_{subject}_interviewAudioTranscript_{interview_type}_day{day:04d}_session*.txt"
    transcript_files = transcript_dir.glob(transcript_name)
    transcript_files = list(transcript_files)

    if len(transcript_files) > 0:
        return "exists"

    prescreening_dir = transcript_dir / "prescreening"
    prescreening_files = prescreening_dir.glob(transcript_name)
    prescreening_files = list(prescreening_files)

    if len(prescreening_files) > 0:
        return "prescreening"

    return "missing"

def add_transcript_files_status(
    status_df: pd.DataFrame,
    data_root: Path
) -> pd.DataFrame:
    """
    Adds the transcript status to the DataFrame.

    Args:
        status_df (pd.DataFrame): The DataFrame containing the status of the interviews.
    """
    status_df.reset_index(drop=True, inplace=True)

    for idx, row in status_df.iterrows():
        subject = row["subject_id"]
        study = row["study_id"]
        interview_type = row["interview_type"]
        day = row["day"]

        transcript_status = check_transcript_status(
            study=study,
            subject=subject,
            interview_type=interview_type,
            day=day,
            data_dir=data_root
        )
        status_df.at[idx, "transcript_status"] = transcript_status

    return status_df

def add_qc_to_df(
    study: str,
    subject: str,
    interview_type: str,
    qc_df: pd.DataFrame,
    status_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Adds the QC status to the DataFrame.

    QC has the following status:
        - pass
        - fail

    Pass is when the overall_db is greater than 40.

    Args:
        study (str): The study ID.
        subject (str): The subject ID.
        interview_type (str): The interview type.
        qc_df (pd.DataFrame): The DataFrame containing the QC data.
        status_df (pd.DataFrame): The DataFrame containing the status of the interviews.

    Returns:
        pd.DataFrame: The DataFrame containing the status of the interviews.
    """

    for _, row in qc_df.iterrows():
        day = row["day"]
        overall_db = row["overall_db"]
        length_mins = row["length_minutes"]

        if overall_db > 40:
            qc_status = "pass"
        else:
            qc_status = "fail"

        df_idxs = status_df[
            (status_df["study_id"] == study) &
            (status_df["subject_id"] == subject) &
            (status_df["interview_type"] == interview_type) &
            (status_df["day"] == day)
        ].index

        for df_idx in df_idxs:
            status_df.at[df_idx, "interview_length_minutes"] = length_mins
            status_df.at[df_idx, "qc_status"] = qc_status   

        if len(df_idxs) == 0:
            interview_name = dpdash.get_dpdash_name(
                study=study,
                subject=subject,
                data_type="interview",
                category=interview_type,
                time_range=f"day{day:04d}"
            )
            logger.warning(
                f"QC data found for {interview_name} but no transcription info found."
            )
            data = []
            data.append({
                "subject_id": subject,
                "study_id": study,
                "interview_type": interview_type,
                "interview_name": interview_name,
                "day": day,
                "interview_length_minutes": length_mins,
                "qc_status": qc_status
            })
            status_df = pd.concat([status_df, pd.DataFrame(data)])

    return status_df

def add_qc_status(
    status_df: pd.DataFrame,
    data_root: Path,
    network: str
) -> pd.DataFrame:
    """
    Adds the QC status to the DataFrame.

    Args:
        status_df (pd.DataFrame): The DataFrame containing the status of the interviews.
    """
    general_dir = data_root / "GENERAL"

    studies = general_dir.iterdir()
    studies = [s.name for s in studies if s.is_dir() and s.name.startswith(network)]

    for study in studies:
        study_dir = general_dir / study
        interviews_dir = study_dir / "processed"
        if not interviews_dir.exists():
            continue

        subjects = interviews_dir.iterdir()
        subjects = [s.name for s in subjects if s.is_dir()]

        for subject in subjects:
            subject_dir = interviews_dir / subject
            interview_dir = subject_dir / "interviews"

            for interview_type in ["open", "psychs"]:
                interview_type_dir = interview_dir / interview_type
                if not interview_type_dir.exists():
                    continue

                audio_qc_files = interview_type_dir.glob("*interviewMonoAudioQC*.csv")
                audio_qc_files = list(audio_qc_files)

                if len(audio_qc_files) == 0:
                    continue

                qc_file = audio_qc_files[0]
                qc_df = pd.read_csv(qc_file)

                status_df = add_qc_to_df(
                    study=study,
                    subject=subject,
                    interview_type=interview_type,
                    qc_df=qc_df,
                    status_df=status_df
                )

    return status_df


def finalize_df(
    status_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Fills in missing values in the DataFrame with meaningful defaults.

    Args:
        status_df (pd.DataFrame): The DataFrame containing the status of the interviews.

    Returns:
        pd.DataFrame: The DataFrame containing the status of the interviews.
    """
    # set the qc_status to missing for all rows where it is nan
    status_df["qc_status"].fillna("missing", inplace=True)

    for idx, row in status_df.iterrows():
        qc_status = row["qc_status"]
        if pd.isna(qc_status):
            status_df.at[idx, "qc_status"] = "missing"

        if qc_status == "nan":
            status_df.at[idx, "qc_status"] = "missing"

    # similarly, set the transcript_status to missing and pipeline_status to unknown
    status_df["transcript_status"].fillna("missing", inplace=True)
    status_df["pipeline_status"].fillna("unknown", inplace=True)

    return status_df



def status_df_to_db(
    config_file: Path,
    status_df: pd.DataFrame
) -> None:
    """
    Inserts the status DataFrame into the database.

    Note: This will delete all existing data in the 'transcription_status' table!

    Args:
        config_file (Path): The path to the configuration file.
        status_df (pd.DataFrame): The DataFrame containing the status of the interviews.

    Returns:
        None
    """
    sql_queries: List[str] = [
        TranscriptionStatus.drop_table_query(),
        TranscriptionStatus.init_table_query()
    ]
    logger.warning(
        "This will delete all existing data in the 'transcription_status' table!"
    )

    for _, row in status_df.iterrows():
        session = row["session"]
        if pd.isna(session):
            session = None
        else:
            session = int(session)

        if pd.isna(row["interview_length_minutes"]):
            interview_length_minutes = None
        else:
            interview_length_minutes = row["interview_length_minutes"]

        transcription_status = TranscriptionStatus(
            subject_id=row["subject_id"],
            study_id=row["study_id"],
            interview_type=row["interview_type"],
            interview_name=row["interview_name"],
            session=session,
            pipeline_status=row["pipeline_status"],
            transcript_file_status=row["transcript_status"],
            qc_status=row["qc_status"],
            interview_length_minutes=interview_length_minutes
        )

        sql_queries.append(transcription_status.to_sql())

    db.execute_queries(
        config_file=config_file, queries=sql_queries, show_commands=False, show_progress=True
    )


if __name__ == "__main__":
    console.rule(f"[bold red]{MODULE_NAME}")

    config_file = utils.get_config_file_path()
    console.print(f"Using config file: {config_file}")

    repo_root = cli.get_repo_root()

    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    config_params = utils.config(config_file, "general")
    data_root = Path(config_params["data_root"])
    network = config_params["network"]
    logger.info(f"Data root: {data_root}")
    logger.info(f"Network: {network}")

    status_df = get_pipeline_status_df(
        data_root=data_root,
        network=network
    )

    status_df = add_transcript_files_status(
        status_df=status_df,
        data_root=data_root
    )

    status_df = add_qc_status(
        status_df=status_df,
        data_root=data_root
    )

    status_df = finalize_df(status_df)
    export_path = repo_root / "data" / "transcription_status.csv"
    logger.info(f"Exporting to {export_path}")
    status_df.to_csv(export_path, index=False)

    console.log(f"Found {len(status_df)} transcript statuses.")

    status_df_to_db(
        config_file=config_file,
        status_df=status_df
    )

    console.log("[bold green]Done!")
