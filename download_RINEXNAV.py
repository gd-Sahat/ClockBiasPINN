import os
import requests
from datetime import datetime, timedelta
from tqdm import tqdm

# Constants
CDDIS_RINEX_BASE = "https://cddis.nasa.gov/archive/gnss/data/daily"
SAVE_DIR = r"D:\MITACS 2025\Code Files\RINEX NAV"

# Convert date to YYYY, YY, DDD
def date_to_parts(dt):
    yyyy = dt.strftime('%Y')
    yy = dt.strftime('%y')
    doy = dt.strftime('%j')  # Day of year (001–366)
    return yyyy, yy, doy

# Download RINEX V2 GPS daily broadcast ephemeris
def download_rinex_gps_file(dt, save_dir):
    yyyy, yy, doy = date_to_parts(dt)
    filename = f"brdc{doy}0.{yy}n.gz"
    url = f"{CDDIS_RINEX_BASE}/{yyyy}/{doy}/{yyn_folder(yy)}/{filename}"

    os.makedirs(save_dir, exist_ok=True)
    local_path = os.path.join(save_dir, filename)

    session = requests.Session()
    session.auth = requests.utils.get_netrc_auth(url)

    try:
        response = session.get(url, stream=True)
        if response.status_code == 200:
            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
        elif response.status_code == 404:
            print(f"Not found: {filename}")
        else:
            print(f"Failed {response.status_code}: {filename}")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
    return False

# Folder for YYn path, e.g., '25n'
def yyn_folder(yy):
    return f"{yy}n"

# Download for a range of dates
def download_range(start_date_str, end_date_str, save_dir):
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]

    print(f"Downloading {len(dates)} daily RINEX V2 GPS BRDC files to '{save_dir}'...")
    for dt in tqdm(dates):
        download_rinex_gps_file(dt, save_dir)

# Run it
if __name__ == "__main__":
    download_range("2023-05-01", "2025-05-01", SAVE_DIR)
