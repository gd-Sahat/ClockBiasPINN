import pandas as pd
import os
import gzip
from tqdm import tqdm

def parse_igs_clock(file_path):
    records = []
    in_data = False

    open_func = gzip.open if file_path.endswith('.gz') else open
    with open_func(file_path, 'rt') as f:
        for line in f:
            if not in_data:
                if 'END OF HEADER' in line:
                    in_data = True
                continue
            if not line.strip():
                continue

            rec_type = line[0:2]
            rec_id = line[3:7].strip()
            year = int(line[8:12])
            month = int(line[13:15])
            day = int(line[16:18])
            hour = int(line[19:21])
            minute = int(line[22:24])
            second = float(line[24:35])

            try:
                num_params = int(line[40:41].strip())
            except ValueError:
                num_params = 2 if rec_type == 'AR' else 3

            bias = drift = drift_rate = None
            start = 41
            if num_params >= 1:
                bias_str = line[start:start+19].strip()
                bias = float(bias_str) if bias_str not in ['', 'NaN'] else None
            if num_params >= 2:
                drift_str = line[start+19:start+38].strip()
                drift = float(drift_str) if drift_str not in ['', 'NaN'] else None
            if num_params >= 3:
                rate_str = line[start+38:start+57].strip()
                drift_rate = float(rate_str) if rate_str not in ['', 'NaN'] else None

            timestamp = pd.Timestamp(year, month, day, hour, minute) + pd.Timedelta(seconds=second)
            records.append({
                'type': rec_type,
                'id': rec_id,
                'timestamp': timestamp,
                'bias_s': bias,
                'drift_s_per_s': drift,
                'drift_rate_s_per_s2': drift_rate
            })

    return pd.DataFrame(records)


def parse_all_igs_clocks_in_directory(input_dir, output_dir, final_csv_name="combined_igs_clock.csv"):
    """
    Parse each IGS clock file into a temp CSV, then merge them all into a combined final CSV.
    """
    clock_files = [
        f for f in os.listdir(input_dir)
        if f.endswith(".CLK") or f.endswith(".CLK.gz")
    ]

    if not clock_files:
        print("❌ No valid .CLK or .CLK.gz files found.")
        return

    os.makedirs(output_dir, exist_ok=True)
    failed_files = []
    temp_csv_paths = []

    print(f"📦 Parsing {len(clock_files)} clock files from: {input_dir}\n")
    for file in tqdm(clock_files, desc="📡 Parsing IGS clock files"):
        full_path = os.path.join(input_dir, file)
        try:
            df = parse_igs_clock(full_path)
            df.sort_values(['timestamp', 'type', 'id'], inplace=True)
            temp_path = os.path.join(output_dir, f"temp_{file}.csv")
            df.to_csv(temp_path, index=False)
            temp_csv_paths.append(temp_path)
        except Exception as e:
            failed_files.append(file)
            print(f"⚠️ Error parsing {file}: {e}")

    if temp_csv_paths:
        print("\n📊 Merging all parsed files into a single combined CSV...")
        final_df = pd.concat([pd.read_csv(p) for p in tqdm(temp_csv_paths)], ignore_index=True)
        final_df.sort_values(['timestamp', 'type', 'id'], inplace=True)
        final_path = os.path.join(output_dir, final_csv_name)
        final_df.to_csv(final_path, index=False)
        print(f"\n✅ Combined CSV saved to: {final_path}")
    else:
        print("❌ No data available for final CSV.")

    if failed_files:
        fail_log = os.path.join(output_dir, "failed_igs_clock_files.txt")
        with open(fail_log, "w") as f:
            for fname in failed_files:
                f.write(fname + "\n")
        print(f"⚠️ {len(failed_files)} files failed. See log: {fail_log}")



# Example usage
if __name__ == "__main__":
    input_folder = r"D:\MITACS 2025\Code Files\IGS Final Clock Products\IGSFINEXTRACTED"
    output_folder = r"D:\MITACS 2025\Code Files\IGS Final Clock Products"
    parse_all_igs_clocks_in_directory(input_folder, output_folder)
