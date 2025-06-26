## Data

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