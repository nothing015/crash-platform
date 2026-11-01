#!/bin/bash
# Downloads the NYC Motor Vehicle Collisions CSV and records its size and checksum.
# Run from the repo folder on your laptop (on Windows, in Git Bash): ./scripts/download_data.sh
# Set FORCE=1 to download again even if the file already exists.
set -e

RAW_DIR="data/raw"
RESULTS_DIR="results"
CSV_FILE="$RAW_DIR/crashes.csv"
INFO_FILE="$RESULTS_DIR/data_info.txt"
DATA_URL="https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD"

# data/ is not in git, so a fresh clone does not have these folders yet
mkdir -p "$RAW_DIR" "$RESULTS_DIR"

if [ -f "$CSV_FILE" ] && [ "${FORCE:-0}" != "1" ]; then
  echo "$CSV_FILE already exists, skipping the download (FORCE=1 to download again)."
  exit 0
fi

echo "Downloading the dataset to $CSV_FILE (about 540 MB)..."
# -f fails on HTTP errors, -L follows redirects
curl -fL -o "$CSV_FILE" "$DATA_URL"

echo "Recording file metadata..."
FILE_SIZE=$(ls -lh "$CSV_FILE" | awk '{print $5}')
if command -v sha256sum >/dev/null 2>&1; then
  CHECKSUM=$(sha256sum "$CSV_FILE" | awk '{print $1}')
else
  CHECKSUM=$(shasum -a 256 "$CSV_FILE" | awk '{print $1}')   # macOS
fi

{
  echo "Download Date: $(date)"
  echo "File Size: $FILE_SIZE"
  echo "SHA256 Checksum: $CHECKSUM"
} > "$INFO_FILE"

echo "Done. Check it with: ls -lh data/raw && head -3 data/raw/crashes.csv"