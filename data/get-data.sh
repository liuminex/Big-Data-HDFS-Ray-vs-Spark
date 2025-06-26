#!/bin/bash

URL="http://konect.cc/files/download.tsv.twitter.tar.bz2"
TARBALL="download.tsv.twitter.tar.bz2"
OUTPUT_DIR="twitter"

mkdir -p "$OUTPUT_DIR"

echo "Downloading Twitter dataset from KONECT..."
wget -O "$TARBALL" "$URL"

if [[ $? -ne 0 ]]; then
    echo "❌ Download failed. Check URL or internet connection."
    exit 1
fi

echo "Extracting dataset..."
tar -xvjf "$TARBALL" -C "$OUTPUT_DIR"

if [[ $? -ne 0 ]]; then
    echo "Extraction failed."
    exit 1
fi

echo "Dataset downloaded and extracted to $OUTPUT_DIR."