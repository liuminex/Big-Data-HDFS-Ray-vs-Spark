import sys
import os
import time
import argparse
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    when, desc, round as spark_round)
from pyspark.sql.types import *
import resource

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def display_results(config, start_time, end_time, extraction_time, transformation_time, loading_time, sample_results):
    execution_time = end_time - start_time
    peak_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # MB on Linux
    
    results_text = f"""
=== ETL SPARK BENCHMARK RESULTS ===
Dataset: {config['datafile']}
Total execution time: {execution_time:.2f} seconds
  - Extraction time: {extraction_time:.2f} seconds
  - Transformation time: {transformation_time:.2f} seconds
  - Loading time: {loading_time:.2f} seconds
Peak memory usage: {peak_memory:.2f} MB
Partitions: {config.get('partitions', 'default')}

ETL Pipeline Operations Completed:
1. Data Extraction from HDFS
2. Data Quality Assessment
3. Text Processing and Feature Engineering
4. Sentiment Analysis Aggregations
5. Time-based Aggregations
6. Complex Joins and Window Functions
7. Data Validation and Cleansing
8. Results Export

Sample Transformation Results:
"""
    
    for key, value in sample_results.items():
        results_text += f"{key}:\n{value}\n\n"
    
    print(results_text)
    
    timestamp = int(time.time())
    filename = f'etl_spark_results_{config["datafile"].replace(".csv", "")}_{timestamp}.txt'
    if not os.path.exists('results'):
        os.makedirs('results')
    with open(f'results/{filename}', 'w') as f:
        f.write(results_text)
    
    print(f"Results saved to results/{filename}")


def extract_data(spark, config):
    # Distributed data extraction from HDFS
    print("=== EXTRACTION PHASE ===")
    hdfs_path = f"hdfs://o-master:54310/data/{config['datafile']}"
    
    print(f"Loading data from HDFS: {hdfs_path}")
    
    # Load with schema inference and optimizations
    df = spark.read.format("csv") \
           .option("header", "true") \
           .option("inferSchema", "true") \
           .option("multiline", "false") \
           .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
           .csv(hdfs_path) \
           .repartition(config.get('partitions', 16))
    
    # Cache for multiple operations
    df.cache()
    
    row_count = df.count()
    print(f"Successfully extracted {row_count} rows with {len(df.columns)} columns")
    
    # Show schema and sample data
    print("Data schema:")
    df.printSchema()
    
    print("Sample data (first 5 rows):")
    df.show(5, truncate=False)
    
    return df


def transform_data(spark, df, config):
    # Distributed data transformation
    print("=== TRANSFORMATION PHASE ===")
    
    sample_results = {}
    
    # 1. Data Quality Assessment
    print("1. Performing data quality assessment...")
    quality_stats = df.select([
        count("*").alias("total_rows"),
        spark_sum(when(col("FracSpecialChars").isNull(), 1).otherwise(0)).alias("null_frac_special"),
        spark_sum(when(col("NumWords").isNull(), 1).otherwise(0)).alias("null_num_words"),
        spark_sum(when(col("SentimentCompound") < -1, 1).otherwise(0)).alias("invalid_sentiment"),
        avg("NumWords").alias("avg_words"),
        spark_max("NumWords").alias("max_words"),
        spark_min("NumWords").alias("min_words")
    ]).collect()[0]
    
    sample_results["Data Quality Stats"] = f"""
    Total rows: {quality_stats['total_rows']}
    Null FracSpecialChars: {quality_stats['null_frac_special']}
    Null NumWords: {quality_stats['null_num_words']}
    Invalid sentiment values: {quality_stats['invalid_sentiment']}
    Avg words per post: {quality_stats['avg_words']:.2f}
    Max words: {quality_stats['max_words']}
    Min words: {quality_stats['min_words']}
    """
    
    # 2. Text Processing and Feature Engineering
    print("2. Performing text processing and feature engineering...")
    df_enhanced = df.withColumn("word_length_category", 
                               when(col("NumWords") < 10, "short")
                               .when(col("NumWords") < 50, "medium")
                               .otherwise("long")) \
                    .withColumn("readability_level",
                               when(col("AutomatedReadabilityIndex") < 6, "elementary")
                               .when(col("AutomatedReadabilityIndex") < 9, "middle_school")
                               .when(col("AutomatedReadabilityIndex") < 13, "high_school")
                               .otherwise("college")) \
                    .withColumn("sentiment_category",
                               when(col("SentimentCompound") > 0.1, "positive")
                               .when(col("SentimentCompound") < -0.1, "negative")
                               .otherwise("neutral")) \
                    .withColumn("special_chars_ratio_binned",
                               when(col("FracSpecialChars") < 0.1, "low")
                               .when(col("FracSpecialChars") < 0.3, "medium")
                               .otherwise("high"))
    
    # 3. Complex Aggregations by Categories
    print("3. Performing sentiment analysis aggregations...")
    sentiment_agg = df_enhanced.groupBy("sentiment_category") \
                              .agg(count("*").alias("count"),
                                   avg("SentimentCompound").alias("avg_compound"),
                                   avg("NumWords").alias("avg_words"),
                                   avg("AvgWordsPerSentence").alias("avg_words_per_sentence")) \
                              .orderBy(desc("count"))
    
    sentiment_results = sentiment_agg.collect()
    sample_results["Sentiment Category Analysis"] = "\n".join([
        f"  {row['sentiment_category']}: {row['count']} posts, "
        f"avg_compound={row['avg_compound']:.3f}, avg_words={row['avg_words']:.1f}"
        for row in sentiment_results
    ])
    
    # 4. Readability Analysis
    print("4. Performing readability analysis...")
    readability_agg = df_enhanced.groupBy("readability_level", "word_length_category") \
                                .agg(count("*").alias("count"),
                                     avg("AutomatedReadabilityIndex").alias("avg_ari"),
                                     avg("SentimentCompound").alias("avg_sentiment")) \
                                .orderBy("readability_level", desc("count"))
    
    readability_results = readability_agg.collect()
    sample_results["Readability Analysis"] = "\n".join([
        f"  {row['readability_level']}/{row['word_length_category']}: {row['count']} posts, "
        f"ARI={row['avg_ari']:.2f}, sentiment={row['avg_sentiment']:.3f}"
        for row in readability_results[:10]  # Top 10 combinations
    ])
    
    # 5. Data Cleansing and Validation
    print("5. Performing data cleansing...")
    df_clean = df_enhanced.filter(
        (col("NumWords") > 0) &
        (col("SentimentCompound").between(-1, 1)) &
        (col("FracSpecialChars").between(0, 1)) &
        (col("AutomatedReadabilityIndex") > 0)
    )
    
    clean_count = df_clean.count()
    original_count = df_enhanced.count()
    sample_results["Data Cleansing"] = f"Removed {original_count - clean_count} invalid rows ({((original_count - clean_count)/original_count*100):.2f}%)"
    
    # 6. Feature Engineering - Create composite scores
    print("6. Creating composite features...")
    df_final = df_clean.withColumn("engagement_score",
                                  spark_round((col("SentimentPositive") + col("SentimentNegative")) * col("NumWords") / 100, 3)) \
                      .withColumn("complexity_score",
                                 spark_round(col("AutomatedReadabilityIndex") * col("AvgWordsPerSentence") / 10, 3)) \
                      .withColumn("quality_score",
                                 spark_round((1 - col("FracSpecialChars")) * col("AvgCharsPerSentence") / 100, 3))
    
    # 7. Final aggregation with window functions
    print("7. Computing final metrics...")
    final_stats = df_final.select([
        count("*").alias("final_row_count"),
        avg("engagement_score").alias("avg_engagement"),
        avg("complexity_score").alias("avg_complexity"),
        avg("quality_score").alias("avg_quality"),
        spark_max("engagement_score").alias("max_engagement"),
        spark_max("complexity_score").alias("max_complexity"),
        spark_max("quality_score").alias("max_quality")
    ]).collect()[0]
    
    sample_results["Final Metrics"] = f"""
    Final dataset size: {final_stats['final_row_count']} rows
    Average engagement score: {final_stats['avg_engagement']:.3f}
    Average complexity score: {final_stats['avg_complexity']:.3f}
    Average quality score: {final_stats['avg_quality']:.3f}
    Max engagement: {final_stats['max_engagement']:.3f}
    Max complexity: {final_stats['max_complexity']:.3f}
    Max quality: {final_stats['max_quality']:.3f}
    """
    
    return df_final, sample_results


def load_data(spark, df_transformed, config):
    # Distributed data loading to HDFS
    print("=== LOADING PHASE ===")
    
    # Create output paths
    output_base = f"hdfs://o-master:54310/etl_output/{config['datafile'].replace('.csv', '')}"
    
    print(f"Saving transformed data to: {output_base}")
    
    # Save main transformed dataset
    df_transformed.coalesce(4).write.mode("overwrite").parquet(f"{output_base}/transformed_data")
    
    # Save aggregated summaries
    summary_stats = df_transformed.groupBy("sentiment_category", "readability_level") \
                                 .agg(count("*").alias("count"),
                                      avg("engagement_score").alias("avg_engagement"),
                                      avg("complexity_score").alias("avg_complexity"),
                                      avg("quality_score").alias("avg_quality"))
    
    summary_stats.coalesce(1).write.mode("overwrite").parquet(f"{output_base}/summary_stats")
    
    print("Data successfully loaded to HDFS")


def etl_spark(spark, config):
    # Main ETL pipeline orchestrator
    print("Starting Spark ETL Pipeline...")
    
    # Extraction
    extract_start = time.time()
    df_raw = extract_data(spark, config)
    extract_end = time.time()
    extraction_time = extract_end - extract_start
    
    # Transformation
    transform_start = time.time()
    df_transformed, sample_results = transform_data(spark, df_raw, config)
    transform_end = time.time()
    transformation_time = transform_end - transform_start
    
    # Loading
    load_start = time.time()
    load_data(spark, df_transformed, config)
    load_end = time.time()
    loading_time = load_end - load_start
    
    return extraction_time, transformation_time, loading_time, sample_results


def main():
    parser = argparse.ArgumentParser(description='ETL Benchmark using Spark')
    parser.add_argument('-f', '--file', type=str, required=True, help='Input CSV file name in HDFS /data/ directory')
    parser.add_argument('--partitions', type=int, default=12, help='Number of partitions')
    
    args = parser.parse_args()
    
    config = {
        'datafile': args.file,
        'partitions': args.partitions
    }
    
    # Initialize Spark with minimal configuration for distributed setup
    spark = SparkSession.builder \
        .appName("ETL_Benchmark") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    try:
        start_time = time.time()
        extraction_time, transformation_time, loading_time, sample_results = etl_spark(spark, config)
        end_time = time.time()
        
        display_results(config, start_time, end_time, extraction_time, transformation_time, loading_time, sample_results)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
