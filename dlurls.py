import os
import logging
import requests
import backoff
import click
import time
from urllib.parse import urlparse
from rich.progress import track
import sys

logging.basicConfig(level=logging.INFO)

def sleep(seconds):
    if seconds:
        for _ in track(range(seconds), description="Sleeping"):
            time.sleep(1)

@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=10)
def download_file(url, base_dir):
    parsed = urlparse(url)
    path = url.replace("https://api.epa.gov/easey/bulk-files/", "")
    local_path = os.path.join(base_dir, path)
    if os.path.exists(local_path):
        print(f"Already exists, skipping.")
        return
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    r = requests.get(url, stream=True)
    try:
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if r.status_code == 429 or r.status_code == 503:
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                print(
                    f"Retry-After header found, waiting for {retry_after} seconds"
                )
                sleep(int(retry_after))
                raise requests.exceptions.RequestException("Retrying after waiting")
        print(f"Error downloading {url}, headers: {r.headers}")
        raise e
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    sys.stdout.write("Done\n")
    sys.stdout.flush()
    return local_path


def main(urls, base_path="/Users/willf/projects/dlurls"):
    l = len(urls)
    for i, url in enumerate(urls):
        sys.stdout.write(f"Downloading {i+1}/{l} {(100.00*(i+1)/l):.2f}%: {url} ... ")
        sys.stdout.flush()
        download_file(url, base_path)



@click.command()
@click.option(
    "--url-file",
    type=click.Path(exists=True),
    required=True,
    help="Path to a file containing URLs.",
)
@click.option(
    "--output-path",
    type=click.Path(file_okay=False, dir_okay=True),
    default="/tmp/data",
    help="Directory to save downloads.",
)
def cli(url_file, output_path):
    with open(url_file, "r") as f:
        urls = [line.strip() for line in f if line.strip()]
    main(urls, base_path=output_path)


if __name__ == "__main__":
    cli()
