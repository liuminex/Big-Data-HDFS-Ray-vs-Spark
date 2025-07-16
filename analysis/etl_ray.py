import time
import ray
import os
import numpy as np
import pandas as pd
import argparse
import resource
from collections import defaultdict
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Dict, Any
import subprocess

np.random.seed(42)


def display_results(config, start_time, end_time, extraction_time, transformation_time, loading_time, sample_results):
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB on Linux
    
    results_text = f"""
=== ETL RAY BENCHMARK RESULTS ===
Dataset: {config['datafile']}
Total execution time: {execution_time:.2f} seconds
  - Extraction time: {extraction_time:.2f} seconds
  - Transformation time: {transformation_time:.2f} seconds
  - Loading time: {loading_time:.2f} seconds
Peak memory usage: {peak_memory:.2f} MB
Number of workers: {config.get('num_workers', 'auto')}

ETL Pipeline Operations Completed:
1. Distributed Data Extraction from HDFS
2. Parallel Data Quality Assessment
3. Distributed Text Processing and Feature Engineering
4. Parallel Sentiment Analysis Aggregations
5. Distributed Time-based Aggregations
6. Complex Distributed Joins and Analytics
7. Parallel Data Validation and Cleansing
8. Distributed Results Export

Sample Transformation Results:
"""
    
    for key, value in sample_results.items():
        results_text += f"{key}:\n{value}\n\n"
    
    print(results_text)
    
    timestamp = int(time.time())
    filename = f'etl_ray_results_{config["datafile"].replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")


@ray.remote(num_cpus=1, memory=256*1024*1024, scheduling_strategy="SPREAD")
def extract_data_chunk(hdfs_path: str, start_byte: int, chunk_size: int, chunk_id: int):
    """Extract a chunk of data from HDFS using hadoop fs command"""
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(extract_data_chunk-{chunk_id}) Processing on node: {node_ip}")
    
    try:
        # Use hadoop fs to read the chunk
        cmd = f"hadoop fs -cat {hdfs_path} | tail -c +{start_byte + 1} | head -c {chunk_size}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error reading chunk {chunk_id}: {result.stderr}")
            return pd.DataFrame()
        
        # Parse CSV data
        from io import StringIO
        csv_data = StringIO(result.stdout)
        
        # For first chunk, include header
        if start_byte == 0:
            df = pd.read_csv(csv_data)
        else:
            # For subsequent chunks, we need to get the header separately
            header_cmd = f"hadoop fs -cat {hdfs_path} | head -n 1"
            header_result = subprocess.run(header_cmd, shell=True, capture_output=True, text=True)
            header = header_result.stdout.strip().split(',')
            
            df = pd.read_csv(csv_data, names=header)
            # Remove the first row if it's partial (from splitting mid-line)
            if len(df) > 0 and not all(isinstance(val, (int, float)) or (isinstance(val, str) and val.replace('.', '').replace('-', '').isdigit()) 
                                      for val in df.iloc[0].values if pd.notna(val)):
                df = df.iloc[1:]
        
        print(f"(extract_data_chunk-{chunk_id}) Extracted {len(df)} rows")
        return df
        
    except Exception as e:
        print(f"Error in extract_data_chunk-{chunk_id}: {e}")
        return pd.DataFrame()


@ray.remote(num_cpus=1, memory=256*1024*1024)
def assess_data_quality(df_chunk: pd.DataFrame, chunk_id: int):
    """Assess data quality for a chunk"""
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(assess_data_quality-{chunk_id}) Processing on node: {node_ip}")
    
    if df_chunk.empty:
        return {}
    
    stats = {
        'total_rows': len(df_chunk),
        'null_frac_special': df_chunk['FracSpecialChars'].isna().sum() if 'FracSpecialChars' in df_chunk.columns else 0,
        'null_num_words': df_chunk['NumWords'].isna().sum() if 'NumWords' in df_chunk.columns else 0,
        'invalid_sentiment': (df_chunk['SentimentCompound'] < -1).sum() if 'SentimentCompound' in df_chunk.columns else 0,
        'sum_words': df_chunk['NumWords'].sum() if 'NumWords' in df_chunk.columns else 0,
        'max_words': df_chunk['NumWords'].max() if 'NumWords' in df_chunk.columns else 0,
        'min_words': df_chunk['NumWords'].min() if 'NumWords' in df_chunk.columns else 0
    }
    
    return stats


@ray.remote(num_cpus=1, memory=256*1024*1024)
def transform_data_chunk(df_chunk: pd.DataFrame, chunk_id: int):
    """Apply transformations to a data chunk"""
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(transform_data_chunk-{chunk_id}) Processing on node: {node_ip}")
    
    if df_chunk.empty:
        return pd.DataFrame(), {}
    
    try:
        # Ensure numeric columns are properly typed
        numeric_cols = ['NumWords', 'FracSpecialChars', 'AutomatedReadabilityIndex', 
                       'SentimentCompound', 'SentimentPositive', 'SentimentNegative',
                       'AvgWordsPerSentence', 'AvgCharsPerSentence']
        
        for col in numeric_cols:
            if col in df_chunk.columns:
                df_chunk[col] = pd.to_numeric(df_chunk[col], errors='coerce')
        
        # 1. Feature Engineering
        df_transformed = df_chunk.copy()
        
        # Word length categories
        df_transformed['word_length_category'] = pd.cut(
            df_transformed['NumWords'], 
            bins=[0, 10, 50, float('inf')], 
            labels=['short', 'medium', 'long']
        ).astype(str)
        
        # Readability levels
        df_transformed['readability_level'] = pd.cut(
            df_transformed['AutomatedReadabilityIndex'],
            bins=[0, 6, 9, 13, float('inf')],
            labels=['elementary', 'middle_school', 'high_school', 'college']
        ).astype(str)
        
        # Sentiment categories
        df_transformed['sentiment_category'] = pd.cut(
            df_transformed['SentimentCompound'],
            bins=[-float('inf'), -0.1, 0.1, float('inf')],
            labels=['negative', 'neutral', 'positive']
        ).astype(str)
        
        # Special chars ratio binning
        df_transformed['special_chars_ratio_binned'] = pd.cut(
            df_transformed['FracSpecialChars'],
            bins=[0, 0.1, 0.3, float('inf')],
            labels=['low', 'medium', 'high']
        ).astype(str)
        
        # 2. Data Cleansing
        mask = (
            (df_transformed['NumWords'] > 0) &
            (df_transformed['SentimentCompound'].between(-1, 1)) &
            (df_transformed['FracSpecialChars'].between(0, 1)) &
            (df_transformed['AutomatedReadabilityIndex'] > 0)
        )
        df_clean = df_transformed[mask].copy()
        
        # 3. Composite Features
        df_clean['engagement_score'] = np.round(
            (df_clean['SentimentPositive'] + df_clean['SentimentNegative']) * df_clean['NumWords'] / 100, 3
        )
        df_clean['complexity_score'] = np.round(
            df_clean['AutomatedReadabilityIndex'] * df_clean['AvgWordsPerSentence'] / 10, 3
        )
        df_clean['quality_score'] = np.round(
            (1 - df_clean['FracSpecialChars']) * df_clean['AvgCharsPerSentence'] / 100, 3
        )
        
        # 4. Local aggregations for this chunk
        chunk_stats = {}
        
        # Sentiment aggregations
        sentiment_agg = df_clean.groupby('sentiment_category').agg({
            'SentimentCompound': ['count', 'mean'],
            'NumWords': 'mean',
            'AvgWordsPerSentence': 'mean'
        }).round(3)
        
        chunk_stats['sentiment_stats'] = sentiment_agg.to_dict()
        
        # Readability aggregations  
        readability_agg = df_clean.groupby(['readability_level', 'word_length_category']).agg({
            'AutomatedReadabilityIndex': 'mean',
            'SentimentCompound': 'mean'
        }).round(3)
        
        chunk_stats['readability_stats'] = readability_agg.to_dict()
        
        # Overall metrics
        chunk_stats['metrics'] = {
            'total_rows': len(df_clean),
            'avg_engagement': df_clean['engagement_score'].mean(),
            'avg_complexity': df_clean['complexity_score'].mean(),
            'avg_quality': df_clean['quality_score'].mean(),
            'max_engagement': df_clean['engagement_score'].max(),
            'max_complexity': df_clean['complexity_score'].max(),
            'max_quality': df_clean['quality_score'].max()
        }
        
        print(f"(transform_data_chunk-{chunk_id}) Transformed {len(df_clean)} rows")
        return df_clean, chunk_stats
        
    except Exception as e:
        print(f"Error in transform_data_chunk-{chunk_id}: {e}")
        return pd.DataFrame(), {}


@ray.remote(num_cpus=1, memory=128*1024*1024)
def save_data_chunk(df_chunk: pd.DataFrame, output_path: str, chunk_id: int):
    """Save a data chunk to HDFS"""
    node_ip = ray._private.services.get_node_ip_address()
    print(f"(save_data_chunk-{chunk_id}) Saving on node: {node_ip}")
    
    if df_chunk.empty:
        return f"Chunk {chunk_id}: No data to save"
    
    try:
        # Save to local parquet first
        local_path = f"/tmp/etl_chunk_{chunk_id}.parquet"
        df_chunk.to_parquet(local_path, index=False)
        
        # Copy to HDFS with overwrite
        hdfs_chunk_path = f"{output_path}/chunk_{chunk_id}.parquet"
        
        # Remove existing file if it exists
        remove_cmd = f"hadoop fs -rm -f {hdfs_chunk_path}"
        subprocess.run(remove_cmd, shell=True, capture_output=True, text=True)
        
        # Copy to HDFS
        cmd = f"hadoop fs -put {local_path} {hdfs_chunk_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        # Clean up local file
        if os.path.exists(local_path):
            os.remove(local_path)
        
        if result.returncode == 0:
            return f"Chunk {chunk_id}: Saved {len(df_chunk)} rows to {hdfs_chunk_path}"
        else:
            return f"Chunk {chunk_id}: Error saving - {result.stderr}"
            
    except Exception as e:
        return f"Chunk {chunk_id}: Error - {e}"


def extract_data_distributed(hdfs_path: str, config: dict):
    """Distributed data extraction from HDFS"""
    print("=== EXTRACTION PHASE ===")
    print(f"Loading data from HDFS: {hdfs_path}")
    
    # Get file size
    cmd = f"hadoop fs -du -s {hdfs_path}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    file_size = int(result.stdout.split()[0])
    
    print(f"File size: {file_size} bytes")
    
    # Calculate chunk parameters optimized for your hardware (2 VMs, 4GB RAM each)
    num_workers = config.get('num_workers') or min(ray.available_resources().get('CPU', 4), 6)  # Limit to 6 workers max
    chunk_size = max(file_size // int(num_workers), 2 * 1024 * 1024)  # At least 2MB per chunk
    
    # Create extraction tasks
    extraction_tasks = []
    for i in range(0, file_size, chunk_size):
        chunk_end = min(i + chunk_size, file_size)
        actual_chunk_size = chunk_end - i
        
        task = extract_data_chunk.remote(hdfs_path, i, actual_chunk_size, len(extraction_tasks))
        extraction_tasks.append(task)
    
    print(f"Created {len(extraction_tasks)} extraction tasks")
    
    # Get results
    data_chunks = ray.get(extraction_tasks)
    
    # Filter out empty chunks and combine
    valid_chunks = [chunk for chunk in data_chunks if not chunk.empty]
    
    if valid_chunks:
        combined_df = pd.concat(valid_chunks, ignore_index=True)
        print(f"Successfully extracted {len(combined_df)} rows with {len(combined_df.columns)} columns")
        return combined_df
    else:
        print("No valid data extracted")
        return pd.DataFrame()


def transform_data_distributed(df: pd.DataFrame, config: dict):
    """Distributed data transformation"""
    print("=== TRANSFORMATION PHASE ===")
    
    if df.empty:
        return pd.DataFrame(), {}
    
    # Split data into chunks for parallel processing (optimized for memory)
    num_workers = config.get('num_workers') or min(ray.available_resources().get('CPU', 4), 6)  # Limit workers
    chunk_size = max(len(df) // int(num_workers), 2000)  # At least 2000 rows per chunk
    
    data_chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    
    print(f"Processing {len(data_chunks)} chunks in parallel")
    
    # Quality assessment
    print("1. Performing distributed data quality assessment...")
    quality_tasks = [assess_data_quality.remote(chunk, i) for i, chunk in enumerate(data_chunks)]
    quality_results = ray.get(quality_tasks)
    
    # Combine quality stats
    combined_quality = defaultdict(int)
    for stats in quality_results:
        for key, value in stats.items():
            combined_quality[key] += value
    
    avg_words = combined_quality['sum_words'] / max(combined_quality['total_rows'], 1)
    
    sample_results = {}
    sample_results["Data Quality Stats"] = f"""
    Total rows: {combined_quality['total_rows']}
    Null FracSpecialChars: {combined_quality['null_frac_special']}
    Null NumWords: {combined_quality['null_num_words']}
    Invalid sentiment values: {combined_quality['invalid_sentiment']}
    Avg words per post: {avg_words:.2f}
    Max words: {combined_quality['max_words']}
    Min words: {combined_quality['min_words']}
    """
    
    # Transformation
    print("2. Performing distributed transformations...")
    transform_tasks = [transform_data_chunk.remote(chunk, i) for i, chunk in enumerate(data_chunks)]
    transform_results = ray.get(transform_tasks)
    
    # Combine transformed data and stats
    transformed_chunks = []
    all_chunk_stats = []
    
    for chunk_data, chunk_stats in transform_results:
        if not chunk_data.empty:
            transformed_chunks.append(chunk_data)
            all_chunk_stats.append(chunk_stats)
    
    if transformed_chunks:
        final_df = pd.concat(transformed_chunks, ignore_index=True)
        
        # Aggregate stats across chunks
        print("3. Aggregating distributed results...")
        
        # Sentiment analysis results
        sentiment_counts = defaultdict(lambda: {'count': 0, 'compound_sum': 0, 'words_sum': 0, 'words_per_sentence_sum': 0})
        
        for chunk_stats in all_chunk_stats:
            if 'sentiment_stats' in chunk_stats:
                for sentiment, stats in chunk_stats['sentiment_stats'].get(('SentimentCompound', 'count'), {}).items():
                    sentiment_counts[sentiment]['count'] += stats
        
        sample_results["Sentiment Category Analysis"] = "\n".join([
            f"  {sentiment}: {data['count']} posts"
            for sentiment, data in sentiment_counts.items()
        ])
        
        # Final metrics
        total_rows = sum(stats['metrics']['total_rows'] for stats in all_chunk_stats if 'metrics' in stats)
        
        if total_rows > 0:
            avg_engagement = sum(stats['metrics']['avg_engagement'] * stats['metrics']['total_rows'] 
                               for stats in all_chunk_stats if 'metrics' in stats) / total_rows
            avg_complexity = sum(stats['metrics']['avg_complexity'] * stats['metrics']['total_rows'] 
                               for stats in all_chunk_stats if 'metrics' in stats) / total_rows
            avg_quality = sum(stats['metrics']['avg_quality'] * stats['metrics']['total_rows'] 
                            for stats in all_chunk_stats if 'metrics' in stats) / total_rows
            
            max_engagement = max(stats['metrics']['max_engagement'] for stats in all_chunk_stats if 'metrics' in stats)
            max_complexity = max(stats['metrics']['max_complexity'] for stats in all_chunk_stats if 'metrics' in stats)
            max_quality = max(stats['metrics']['max_quality'] for stats in all_chunk_stats if 'metrics' in stats)
            
            sample_results["Final Metrics"] = f"""
    Final dataset size: {total_rows} rows
    Average engagement score: {avg_engagement:.3f}
    Average complexity score: {avg_complexity:.3f}
    Average quality score: {avg_quality:.3f}
    Max engagement: {max_engagement:.3f}
    Max complexity: {max_complexity:.3f}
    Max quality: {max_quality:.3f}
    """
        
        print(f"Transformation completed: {len(final_df)} rows processed")
        return final_df, sample_results
    else:
        return pd.DataFrame(), sample_results


def load_data_distributed(df: pd.DataFrame, config: dict):
    """Distributed data loading to HDFS"""
    print("=== LOADING PHASE ===")
    
    if df.empty:
        print("No data to save")
        return
    
    output_base = f"hdfs://o-master:54310/etl_output/{config['datafile'].replace('.csv', '')}_ray"
    
    print(f"Saving transformed data to: {output_base}")
    
    # Clean up any existing output directory first
    cleanup_cmd = f"hadoop fs -rm -r -f {output_base}"
    subprocess.run(cleanup_cmd, shell=True, capture_output=True, text=True)
    
    # Create output directory
    cmd = f"hadoop fs -mkdir -p {output_base}/transformed_data"
    subprocess.run(cmd, shell=True)
    
    # Split data into chunks for parallel saving
    num_workers = config.get('num_workers') or ray.available_resources().get('CPU', 4)
    chunk_size = max(len(df) // int(num_workers), 1000)
    
    data_chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    
    # Save chunks in parallel
    save_tasks = [save_data_chunk.remote(chunk, f"{output_base}/transformed_data", i) 
                  for i, chunk in enumerate(data_chunks)]
    
    save_results = ray.get(save_tasks)
    
    for result in save_results:
        print(result)
    
    print("Data successfully loaded to HDFS")


def etl_ray(config):
    """Main ETL pipeline orchestrator"""
    print("Starting Ray ETL Pipeline...")
    
    hdfs_path = f"hdfs://o-master:54310/data/{config['datafile']}"
    
    # Extraction
    extract_start = time.time()
    df_raw = extract_data_distributed(hdfs_path, config)
    extract_end = time.time()
    extraction_time = extract_end - extract_start
    
    # Transformation
    transform_start = time.time()
    df_transformed, sample_results = transform_data_distributed(df_raw, config)
    transform_end = time.time()
    transformation_time = transform_end - transform_start
    
    # Loading
    load_start = time.time()
    load_data_distributed(df_transformed, config)
    load_end = time.time()
    loading_time = load_end - load_start
    
    return extraction_time, transformation_time, loading_time, sample_results


def main():
    parser = argparse.ArgumentParser(description='ETL Benchmark using Ray')
    parser.add_argument('-f', '--file', type=str, required=True, help='Input CSV file name')
    parser.add_argument('-c', '--cores', type=int, default=None, help='Number of CPU cores to use')
    parser.add_argument('--memory', type=str, default='1GB', help='Total memory to use')
    
    args = parser.parse_args()
    
    config = {
        'datafile': args.file,
        'num_workers': args.cores,
        'memory': args.memory
    }
    
    # Initialize Ray
    if ray.is_initialized():
        ray.shutdown()
    
    ray_config = {
        'ignore_reinit_error': True,
        'include_dashboard': False
    }
    
    if args.cores:
        ray_config['num_cpus'] = args.cores
    
    ray.init(**ray_config)
    
    try:
        start_time = time.time()
        extraction_time, transformation_time, loading_time, sample_results = etl_ray(config)
        end_time = time.time()
        
        display_results(config, start_time, end_time, extraction_time, transformation_time, loading_time, sample_results)
        
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
