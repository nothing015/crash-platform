#!/bin/bash

# Exit immediately if a command fails
set -e

# Define file paths
RAW_DIR="data/raw"
RESULTS_DIR="results"
CSV_FILE="$RAW_DIR/crashes.csv"
INFO_FILE="$RESULTS_DIR/data_info.txt"

# The NYC Open Data CSV export URL for Motor Vehicle Collisions - Crashes
DATA_URL="https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD"

echo "Downloading the dataset to $CSV_FILE..."
# -L follows redirects, -o specifies the output file
curl -L -o "$CSV_FILE" "$DATA_URL"

echo "Calculating file metadata..."
DOWNLOAD_DATE=$(date)
# Get human-readable file size
FILE_SIZE=$(ls -lh "$CSV_FILE" | awk '{print $5}')
# Calculate SHA256 checksum
CHECKSUM=$(sha256sum "$CSV_FILE" | awk '{print $1}')

echo "Writing metadata to $INFO_FILE..."
echo "Download Date: $DOWNLOAD_DATE" > "$INFO_FILE"
echo "File Size: $FILE_SIZE" >> "$INFO_FILE"
echo "SHA256 Checksum: $CHECKSUM" >> "$INFO_FILE"

echo "Download complete! Run 'ls -lh data/raw && head -3 data/raw/crashes.csv' to verify."