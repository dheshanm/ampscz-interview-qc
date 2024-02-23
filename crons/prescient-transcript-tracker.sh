#!/usr/bin/env bash

source ~/.bash_profile

export REPO_ROOT=/home/dm1447/dev/ampscz-interview-qc
export CONDA_ENV=jupyter
cd $REPO_ROOT

export SEPARATOR="========================================================================================================================"

LOG_FILE=$REPO_ROOT/data/logs/crons/cron-$(date +%Y%m%d%H%M%S).log
exec > >(tee -ia "$LOG_FILE")
exec 2> >(tee -ia "$LOG_FILE" >&2)

echo "$(date) - Starting cron job"
echo "$(date) - Log file: $LOG_FILE"
echo $SEPARATOR
echo "$(date) - Hostname: $(hostname)"
echo "$(date) - User: $(whoami)"
echo ""
echo "$(date) - Current directory: $(pwd)"
echo "$(date) - Git branch: $(git rev-parse --abbrev-ref HEAD)"
echo "$(date) - Git commit: $(git rev-parse HEAD)"
echo "$(date) - Git status: "
echo "$(git status --porcelain)"
echo ""
echo "$(date) - Uptime: $(uptime)"
echo $SEPARATOR

echo "$(date) - Activating conda environment: $CONDA_ENV"
source /home/dm1447/miniforge3/etc/profile.d/conda.sh
conda activate $CONDA_ENV

echo $SEPARATOR
echo "$(date) - Starting PostgreSQL"
pg_ctl -D $REPO_ROOT/data/postgresql -l $REPO_ROOT/data/postgresql/logfile start

echo $SEPARATOR
echo "$(date) - Running prescient-transcript-tracker"
/home/dm1447/dev/ampscz-interview-qc/interviewqc/runners/status/transcription_status.py

echo "$(date) - Done"
