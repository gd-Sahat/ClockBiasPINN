import os
import requests
from datetime import datetime, timedelta
from tqdm import tqdm

# Constants
CDDIS_BASE_URL = "https://cddis.nasa.gov/archive/gnss/products"
SAVE_DIR = r"D:\MITACS 2025\Code Files\IGS Final Clock Products"

# Convert date to GPS week
def date_to_gps_week_day(dt):
    gps_start = datetime(1980, 1, 6)
    delta = dt - gps_start
    gps_week = delta.days // 7
    return gps_week

# Convert date to Year and Day of Year (e.g., 2025123)
def date_to_yeardoy(dt):
    return dt.strftime('%Y'), dt.strftime('%j')

# Download a single IGS Final 30s clock file for a date
def download_igs_clock_file(dt, save_dir):
    gps_week = date_to_gps_week_day(dt)
    yyyy, doy = date_to_yeardoy(dt)
    filename = f"IGS0OPSFIN_{yyyy}{doy}0000_01D_30S_CLK.CLK.gz"
    url = f"{CDDIS_BASE_URL}/{gps_week:04d}/{filename}"

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
            print(f"File not found for {dt.date()}: {filename}")
        else:
            print(f"Failed {response.status_code} for {filename}")
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
    return False

# Download multiple files between start and end dates
def download_range(start_date_str, end_date_str, save_dir):
    start = datetime.strptime(start_date_str, "%Y-%m-%d")
    end = datetime.strptime(end_date_str, "%Y-%m-%d")
    dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]

    print(f"Downloading {len(dates)} IGS 30s clock files to '{save_dir}'...")
    for dt in tqdm(dates):
        download_igs_clock_file(dt, save_dir)

# Run it
if __name__ == "__main__":
    download_range("2023-05-01", "2025-05-01", SAVE_DIR)
