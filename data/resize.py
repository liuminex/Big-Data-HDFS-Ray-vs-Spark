import sys

def reduce_file_size(input_tsv, output_csv, target_size_gb):
    target_size_bytes = target_size_gb * (1024 ** 3)
    size = 0

    with open(input_tsv, 'r') as infile, open(output_csv, 'w') as outfile:
        outfile.write("node1,node2\n")

        for line in infile:
            if line.startswith('%') or not line.strip():
                continue  # Skip comment or empty lines

            parts = line.strip().split('\t')  # KONECT uses tab-separated values
            if len(parts) < 2:
                continue  # Skip malformed lines

            csv_line = f"{parts[0]},{parts[1]}\n"
            outfile.write(csv_line)
            size += len(csv_line.encode('utf-8'))

            if size >= target_size_bytes:
                print(f"Reached target size of {target_size_gb} GB.")
                break

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python resize.py <input_tsv> <target_size_gb>")
        sys.exit(1)
    input_file = sys.argv[1]
    target_size_gb = float(sys.argv[2])
    output_file = f"data_{target_size_gb}G.csv"
    reduce_file_size(input_file, output_file, target_size_gb)