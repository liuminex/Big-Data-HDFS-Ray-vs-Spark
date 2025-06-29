import pandas as pd
import numpy as np
import sys
import os
from tqdm import tqdm
import uuid

def generate_synthetic_reddit_data(target_size_bytes: str, input_file: str):
    output_file = f"data_reddit_{target_size_bytes}.csv"
    print(f"Creating file: {output_file}")

    if target_size_bytes.endswith('G'):
        target_size_bytes = float(target_size_bytes[:-1]) * 1_000_000_000
    elif target_size_bytes.endswith('M'):
        target_size_bytes = float(target_size_bytes[:-1]) * 1_000_000
    elif target_size_bytes.endswith('K'):
        target_size_bytes = float(target_size_bytes[:-1]) * 1_000
    else:
        target_size_bytes = float(target_size_bytes)

    target_size_bytes = int(target_size_bytes)

    print(f"Target size: {target_size_bytes} bytes")

    df = pd.read_csv(input_file)

    original_size = os.path.getsize(input_file)
    print(f"Original file size: {original_size} bytes, rows: {len(df)}")

    # Prepare for sampling
    columns = df.columns.tolist()
    column_values = {col: df[col].values for col in columns}
    existing_ids = set(df['POST_ID'].astype(str).values)

    # Estimate average row size
    sample_row_str = df.iloc[0].to_csv(header=False, index=False).encode('utf-8')
    avg_row_size = len(sample_row_str)
    print(f"Estimated average row size: {avg_row_size} bytes")

    rows_needed = (target_size_bytes - original_size) // avg_row_size
    if rows_needed <= 0:
        print("Target size is less than or equal to original file size. No new rows needed.")
        rows_needed = 0
    else:
        print(f"Generating approximately {rows_needed} synthetic rows.")

    # Write the original DataFrame first
    df.to_csv(output_file, index=False)

    # Open output file in append mode
    with open(output_file, "a", encoding="utf-8") as f_out:
        for _ in tqdm(range(rows_needed), desc="Generating rows"):
            new_row = {}
            for col in columns:
                new_row[col] = np.random.choice(column_values[col])

            # Generate unique POST_ID
            while True:
                new_post_id = uuid.uuid4().hex[:8]
                if new_post_id not in existing_ids:
                    existing_ids.add(new_post_id)
                    new_row['POST_ID'] = new_post_id
                    break

            # Convert row to CSV string
            row_values = [str(new_row[col]).replace('\n', ' ').replace('\r', ' ') for col in columns]
            row_str = ",".join(f'"{val}"' if ',' in val or '"' in val else val for val in row_values)

            # Write the row
            f_out.write("\n" + row_str)

    final_size = os.path.getsize(output_file)
    print(f"Saved data to {output_file}")
    print(f"Final file size: {final_size} bytes")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python generate_reddit_data.py <target_size_in_bytes>")
        sys.exit(1)

    input_csv = "data_reddit_original.csv"

    generate_synthetic_reddit_data(sys.argv[1], input_csv)
