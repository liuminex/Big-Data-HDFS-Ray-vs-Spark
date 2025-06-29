## Data

`data_reddit_original.csv` is the original dataset (70MB). In order to enlarge the dataset you can use the following to create a dataset of any size:
```bash
python3 generate-data.py <target_size_in_bytes>

# examples
python3 generate-data.py 1G
python3 generate-data.py 0.1G
python3 generate-data.py 90M
python3 generate-data.py 1000000000
```

>> Original dataset is from [https://snap.stanford.edu/data/soc-RedditHyperlinks.html](https://snap.stanford.edu/data/soc-RedditHyperlinks.html) ([download link](https://snap.stanford.edu/data/soc-redditHyperlinks-body.tsv)). It was converted to CSV format using the `fix-tsf-to-csv-reddit.py` script.


<hr>
<hr>


Get the dataset used in the project from [KONECT](http://konect.cc/networks/).

## Downloading the Dataset
Run the following command to download the Twitter dataset:

```bash
cd data
./get-data.sh
```

## Resizing the Dataset
You can resize using the `resize.py` script if needed.
```bash
python3 resize.py twitter/out.twitter <target_size_gb>
```
The script will create a new CSV file (e.g. `data_1.0G.csv`) with the specified target size in GB.