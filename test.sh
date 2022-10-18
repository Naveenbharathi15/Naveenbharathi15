#!/usr/bin/env bash

USER="superset"
DB="superset"
TBALE_NAME="upvpp"
CSV_DIR="/home/root0002/Downloads"
FILE_NAME="upvpp.csv"

echo $(psql -d $DB -U $USER  -c "\copy $TBALE_NAME from '$CSV_DIR/$FILE_NAME' DELIMITER E',' csv" 2>&1 |tee /dev/tty)