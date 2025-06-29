## Data

This project uses the Reddit hyperlinks dataset from Stanford SNAP.

## Getting the Dataset

### Step 1: Download and Convert to CSV
Run the following command to download the Reddit dataset from Stanford and convert it to CSV format:

```bash
./get-data.sh
```

This script will:
- Download the original TSV dataset from [Stanford SNAP](https://snap.stanford.edu/data/soc-RedditHyperlinks.html)
- Convert it from TSV to CSV format
- Rename it to `data_reddit_original.csv`

### Step 2: Generate Larger Datasets (Optional)
If you need a larger dataset for testing, you can generate more data using:

```bash
python3 generate-data.py <target_size_in_bytes>
```

**Examples:**
```bash
python3 generate-data.py 1G        # 1 gigabyte
python3 generate-data.py 0.1G      # 100 megabytes  
python3 generate-data.py 90M       # 90 megabytes
python3 generate-data.py 1000000000 # 1GB in bytes
```

This will create enlarged datasets by replicating and modifying the original data.