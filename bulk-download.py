import os
import sys
import requests
import click
from urllib.parse import urlparse
import time
import backoff
from functools import wraps

# Dictionary to store the last call times
last_call_times = {}

def track_time_between_calls(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        key = (args, tuple(kwargs.items()))
        current_time = time.time()
        if key in last_call_times:
            last_call_time = last_call_times[key]
            time_since_last_call = current_time - last_call_time
            sys.stdout.write(f" ({time_since_last_call:.2f} secs)")
        else:
            time_since_last_call = None
            #sys.stdout.write("; First call with these arguments\n")
        last_call_times[key] = current_time
        return func(*args, **kwargs)
    return wrapper

@backoff.on_exception(backoff.expo, Exception, max_tries=15)
@track_time_between_calls
def download_file(url, base_dir,sleep):
    # Parse the URL to get the path
    path = url.replace("https://api.epa.gov/easey/bulk-files/", "")
    file_path = os.path.join(base_dir, path)

    # Create directories if they don't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # if the file exists, skip downloading
    if os.path.exists(file_path):
        sys.stdout.write(f"; File already exists: {file_path}")
        return

    # Download the file
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        sys.stdout.write(f"; Downloaded: {file_path}")
        sys.stdout.flush()
        time.sleep(sleep)
    else:
        sys.stdout.write(f"; Failed to download: {url}")
        sys.stdout.flush()
        raise Exception(f"Failed to download: {url}")

@click.command()
@click.option('--base-dir', required=True, type=click.Path(), help='Base directory to save files')
@click.option('--urls-file', required=True, type=click.Path(exists=True), help='Path to the file containing URLs')
@click.option('--sleep', default=10, help='Number of seconds to sleep between downloads')
def main(base_dir, urls_file,sleep):
    with open(urls_file, 'r') as file:
        urls = file.readlines()
    n_urls = len(urls)
    print(f"Found {n_urls} URLs to download")

    for n, url in enumerate(urls, 1):
        percent = n / n_urls * 100
        sys.stdout.write(f"Reading line {n}/{n_urls}; {percent:.2f}% complete.")
        url = url.strip()
        if not url:
            continue
        if url.startswith("#"):
            continue
        # check if the URL is valid
        is_valid_url = bool(urlparse(url).netloc)
        if not is_valid_url:
            continue
        if url:
            download_file(url, base_dir,sleep)
        sys.stdout.write("\n")
        sys.stdout.flush()

if __name__ == "__main__":
    main()