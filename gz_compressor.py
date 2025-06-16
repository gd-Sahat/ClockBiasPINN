import pandas as pd
import gzip
from tqdm import tqdm

def compress_csv_with_progress(input_csv, output_gz, chunksize=100_000):
    """
    Compress a large CSV file to .gz with a visible progress bar.
    """
    # First, count total lines for progress bar (excluding header)
    with open(input_csv, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f) - 1

    # Stream and compress with progress
    with pd.read_csv(input_csv, chunksize=chunksize) as reader, \
         gzip.open(output_gz, 'wt', encoding='utf-8') as gz_file:

        pbar = tqdm(total=total_lines, desc="🔄 Compressing CSV")
        header_written = False

        for chunk in reader:
            chunk.to_csv(gz_file, index=False, header=not header_written)
            header_written = True
            pbar.update(len(chunk))

        pbar.close()

# Example usage
compress_csv_with_progress(r"D:\MITACS 2025\Code Files\IGS Final Clock Products\combined_igs_clock.csv", r"D:\MITACS 2025\Code Files\IGS Final Clock Products\combined_igs_clock.csv.gz")

