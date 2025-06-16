import os
import pandas as pd
from tqdm import tqdm
import gnss_lib_py as glp

def parse_all_rinex_nav_files(input_dir, output_dir, output_csv="combined_rinex_nav.csv"):
    """
    Parse multiple RINEX navigation files (.nav, .YYn) using gnss_lib_py and export combined CSV.
    """
    nav_files = [
        f for f in os.listdir(input_dir)
        if f.endswith(".nav") or f.endswith(".YYn") or f.endswith(".YYn.gz")
    ]

    if not nav_files:
        print("❌ No RINEX navigation files found.")
        return

    all_dfs = []
    os.makedirs(output_dir, exist_ok=True)

    print(f"📦 Found {len(nav_files)} RINEX nav files to parse in {input_dir}.\n")
    for nav_file in tqdm(nav_files, desc="📡 Parsing RINEX files"):
        full_path = os.path.join(input_dir, nav_file)
        try:
            rinex_nav = glp.RinexNav(full_path)
            df = rinex_nav.pandas_df()
            df['source_file'] = nav_file  # Optional: keep track of origin
            all_dfs.append(df)
        except Exception as e:
            print(f"⚠️ Failed to parse {nav_file}: {e}")

    if not all_dfs:
        print("❌ No files parsed successfully.")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Sort by time and satellite ID (if present)
    sort_cols = ['time'] if 'time' in combined_df.columns else []
    if 'sat_id' in combined_df.columns:
        sort_cols.append('sat_id')
    if sort_cols:
        combined_df.sort_values(by=sort_cols, inplace=True)

    # Save CSV
    output_path = os.path.join(output_dir, output_csv)
    combined_df.to_csv(output_path, index=False)
    print(f"\n✅ Combined RINEX navigation CSV saved to: {output_path}")

# Example usage
if __name__ == "__main__":
    input_folder = r"D:\MITACS 2025\Code Files\RINEX NAV\RINEXNAVEXTRACTED"
    output_folder = r"D:\MITACS 2025\Code Files\RINEX NAV\Processed RINEX CSV"
    parse_all_rinex_nav_files(input_folder, output_folder)
