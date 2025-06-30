import os
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta

GITHUB_RAW_BASE_URL = "https://raw.githubusercontent.com/ErKiran/kalimati/master/data/csv"
START_DATE = datetime(2023, 9, 28)
END_DATE = datetime.today()

def download_csv_file(date_obj, local_path):
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%m")
    day_file = date_obj.strftime("%d.csv")
    url = f"{GITHUB_RAW_BASE_URL}/{year}/{month}/{day_file}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type', '')
            if 'text/plain' in content_type or 'text/csv' in content_type:
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded: {local_path}")
            else:
                print(f"Skipped non-CSV file: {url} [Content-Type: {content_type}]")
        else:
            print(f"File not found: {url}")
    except Exception as e:
        print(f"Error downloading {url}: {e}")

def download_all_data(local_data_dir, overwrite=True):
    current_date = START_DATE
    while current_date <= END_DATE:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day_file = current_date.strftime("%d.csv")
        local_file_path = os.path.join(local_data_dir, year, month, day_file)

        if overwrite or not os.path.exists(local_file_path):
            download_csv_file(current_date, local_file_path)

        current_date += timedelta(days=1)

def collect_csv_files(base_dir):
    data_frames = []
    date_records = []
    bad_files = []

    for year in sorted(os.listdir(base_dir)):
        year_path = os.path.join(base_dir, year)
        if not os.path.isdir(year_path):
            continue

        for month in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue

            for file_name in sorted(os.listdir(month_path)):
                if not file_name.endswith('.csv'):
                    continue

                file_path = os.path.join(month_path, file_name)

                try:
                    df = pd.read_csv(file_path, on_bad_lines='skip')
                    date_str = f"{year}-{month}-{file_name.replace('.csv', '')}"
                    date_obj = pd.to_datetime(date_str, format='%Y-%m-%d', errors='coerce')

                    if pd.isna(date_obj):
                        continue

                    df['Date'] = date_obj
                    data_frames.append(df)
                    date_records.append(date_obj)

                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
                    bad_files.append(file_path)

    return data_frames, date_records, bad_files

def generate_output_filename(start_date, end_date):
    start_str = start_date.strftime("%Y_%m_%d")
    end_str = end_date.strftime("%Y_%m_%d")
    return f"kalimati_tarkari_{start_str}_{end_str}.csv"

def extract_and_combine(local_data_dir):
    all_data, all_dates, bad_files = collect_csv_files(local_data_dir)

    if not all_data:
        print("No valid CSV data found.")
        return

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.sort_values('Date', inplace=True)

    # Reorder columns: Date first
    cols = combined_df.columns.tolist()
    if 'Date' in cols:
        cols.remove('Date')
        cols = ['Date'] + cols
        combined_df = combined_df[cols]

    output_file = generate_output_filename(START_DATE, max(all_dates))
    combined_df.to_csv(output_file, index=False)

    print(f"\nCombined CSV saved as: {output_file}")

    if bad_files:
        print("\nFiles with parsing issues:")
        for bad_file in bad_files:
            print(bad_file)

def main():
    parser = argparse.ArgumentParser(description="Kalimati Tarkari Data Downloader and Combiner")
    parser.add_argument('--update', action='store_true', help='Download all CSV files from GitHub (overwrite existing)')
    parser.add_argument('--update-missing', action='store_true', help='Download only missing CSV files')
    parser.add_argument('--extract', action='store_true', help='Combine existing local CSV files into one output')

    args = parser.parse_args()
    local_data_dir = 'data'

    if args.update:
        print("\n--- Downloading ALL data from GitHub (overwrite mode) ---\n")
        download_all_data(local_data_dir, overwrite=True)

    if args.update_missing:
        print("\n--- Downloading missing CSVs from GitHub ---\n")
        download_all_data(local_data_dir, overwrite=False)

    if args.extract:
        print("\n--- Extracting and combining local CSV files ---\n")
        extract_and_combine(local_data_dir)

    if not args.update and not args.update_missing and not args.extract:
        parser.print_help()

if __name__ == "__main__":
    main()
