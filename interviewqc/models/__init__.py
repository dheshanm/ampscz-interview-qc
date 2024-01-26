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


from typing import List


from interviewqc.helpers import db
from interviewqc.models.file import File
from interviewqc.models.moved_file import MovedFile
from interviewqc.models.site import Site
from interviewqc.models.subject import Subject
from interviewqc.models.interview import Interview
from interviewqc.models.interview_raw import InterviewRaw
from interviewqc.models.transcripts import Transcript


def init_db(config_file: Path):
    drop_queries: List[str] = [
        Transcript.drop_table_query(),
        InterviewRaw.drop_table_query(),
        Interview.drop_table_query(),
        Subject.drop_table_query(),
        Site.drop_table_query(),
        File.drop_table_query(),
        MovedFile.drop_table_query(),
    ]

    init_quries = [
        MovedFile.init_table_query(),
        File.init_table_query(),
        Site.init_table_query(),
        Subject.init_table_query(),
        Interview.init_table_query(),
        InterviewRaw.init_table_query(),
        Transcript.init_table_query(),
    ]

    sql_queries = drop_queries + init_quries

    db.execute_queries(config_file=config_file, queries=sql_queries)
