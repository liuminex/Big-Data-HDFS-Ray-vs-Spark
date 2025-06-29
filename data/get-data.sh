# create a script to download https://snap.stanford.edu/data/soc-RedditHyperlinks.html

#!/bin/bash

# Run this script in both VMs
source ../documentation/scripts/config.sh || { eko RED "config.sh not found."; }
DATA_DIR="$SOURCE_DIR/data"

# Check if wget is installed
if ! command -v wget &> /dev/null
then
    eko RED "wget could not be found. Please install wget to proceed."
    exit 1
else
    eko GREEN "wget is already installed"
fi

# Download the dataset
DATA_URL="https://snap.stanford.edu/data/soc-redditHyperlinks-body.tsv"

if [ ! -f "$DATA_DIR/soc-redditHyperlinks-body.tsv" ]; then
    eko CYAN "Downloading dataset from $DATA_URL ..."
    wget -q "$DATA_URL" -O "$DATA_DIR/soc-redditHyperlinks-body.tsv" || { eko RED "Failed to download dataset."; exit 1; }
    eko GREEN "Dataset downloaded successfully"
else
    eko GREEN "Dataset already exists in $DATA_DIR"
fi

# Check if the dataset is downloaded correctly
if [ -f "$DATA_DIR/soc-redditHyperlinks-body.tsv" ]; then
    eko GREEN "Dataset is available at $DATA_DIR/soc-redditHyperlinks-body.tsv"
else
    eko RED "Dataset download failed. Please check the URL or your internet connection."
    exit 1
fi

# Convert TSV to CSV
eko CYAN "Converting TSV to CSV format..."

# Check if awk is available
if ! command -v awk &> /dev/null; then
    eko RED "awk could not be found. Please install awk to proceed."
    exit 1
fi

# Define output CSV file
CSV_FILE="$DATA_DIR/data_reddit_original.csv"

# Create CSV header
echo "SOURCE_SUBREDDIT,TARGET_SUBREDDIT,POST_ID,TIMESTAMP,LINK_SENTIMENT,NumCharacters,NumCharactersNoSpace,FracAlphabetical,FracDigits,FracUppercase,FracWhiteSpace,FracSpecialChars,NumWords,NumUniqueWords,NumLongWords,AvgWordLength,NumUniqueStopwords,FracStopwords,NumSentences,NumLongSentences,AvgCharsPerSentence,AvgWordsPerSentence,AutomatedReadabilityIndex,SentimentPositive,SentimentNegative,SentimentCompound" > "$CSV_FILE"

# Convert TSV to CSV using awk (skip header line)
awk -F'\t' 'NR > 1 {
    # Split the 6th column (POST_PROPERTIES) by commas
    split($6, properties, ",")
    
    # Print first 5 columns plus first 21 properties from the 6th column
    printf "%s,%s,%s,%s,%s", $1, $2, $3, $4, $5
    
    # Add the first 21 properties from POST_PROPERTIES
    for (i = 1; i <= 21 && i <= length(properties); i++) {
        printf ",%s", properties[i]
    }
    
    # Fill remaining columns with empty values if less than 21 properties
    for (i = length(properties) + 1; i <= 21; i++) {
        printf ","
    }
    
    printf "\n"
}' "$DATA_DIR/soc-redditHyperlinks-body.tsv" >> "$CSV_FILE"

if [ -f "$CSV_FILE" ]; then
    eko GREEN "TSV successfully converted to CSV format"
    eko GREEN "CSV file created at: $CSV_FILE"
else
    eko RED "Failed to convert TSV to CSV"
    exit 1
fi

eko GREEN "Data preparation completed successfully. You can find the CSV file at $CSV_FILE"