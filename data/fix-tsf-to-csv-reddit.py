import pandas as pd

# Input file path
input_file = "soc-redditHyperlinks-body.tsv"

# Base columns
base_columns = [
    "SOURCE_SUBREDDIT",
    "TARGET_SUBREDDIT",
    "POST_ID",
    "TIMESTAMP",
    "POST_LABEL",
    "POST_PROPERTIES"
]

# Read TSV
df = pd.read_csv(
    input_file,
    sep="\t",
    names=base_columns,
    header=None,
    dtype=str,
    quoting=3,
    engine="python"
)

# Split POST_PROPERTIES into list
df["POST_PROPERTIES_LIST"] = df["POST_PROPERTIES"].str.split(",")

# Check length
invalid_rows = df[df["POST_PROPERTIES_LIST"].apply(lambda x: len(x) != 86)]
if not invalid_rows.empty:
    print("⚠️ Warning: Some rows do not have 86 properties.")
    print(invalid_rows)
    # Uncomment to drop them:
    # df = df[df["POST_PROPERTIES_LIST"].apply(lambda x: len(x) == 86)]

# Column names for first 21 properties
text_property_names = [
    "NumCharacters",
    "NumCharactersNoSpace",
    "FracAlphabetical",
    "FracDigits",
    "FracUppercase",
    "FracWhiteSpace",
    "FracSpecialChars",
    "NumWords",
    "NumUniqueWords",
    "NumLongWords",
    "AvgWordLength",
    "NumUniqueStopwords",
    "FracStopwords",
    "NumSentences",
    "NumLongSentences",
    "AvgCharsPerSentence",
    "AvgWordsPerSentence",
    "AutomatedReadabilityIndex",
    "SentimentPositive",
    "SentimentNegative",
    "SentimentCompound"
]

# Extract first 21 properties
for i, name in enumerate(text_property_names):
    df[name] = df["POST_PROPERTIES_LIST"].apply(lambda x: x[i] if len(x) == 86 else None)

# Drop helper columns
df = df.drop(columns=["POST_PROPERTIES", "POST_PROPERTIES_LIST"])

# Reorder columns
output_columns = (
    ["SOURCE_SUBREDDIT", "TARGET_SUBREDDIT", "POST_ID", "TIMESTAMP", "POST_LABEL"]
    + text_property_names
)

df = df[output_columns]

# Save to CSV
output_file = "reddit_hyperlinks_text_properties.csv"
df.to_csv(output_file, index=False)

print(f"✅ CSV created: {output_file}")
