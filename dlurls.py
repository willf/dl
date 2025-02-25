import os
import requests
from loguru import logger

import random

import click
import time
from urllib.parse import urlparse
from rich.progress import track
import sys
import enum
import re
from collections import namedtuple

logger.remove(0)


def humanize_bytes(num_bytes):
    """
    Convert a number of bytes into a human-readable format (e.g., KB, MB, GB).

    :param num_bytes: Number of bytes
    :return: Human-readable string
    """
    if num_bytes < 1024:
        return f"{num_bytes} bytes"
    elif num_bytes < 1024**2:
        return f"{num_bytes / 1024:.2f} Kb"
    elif num_bytes < 1024**3:
        return f"{num_bytes / 1024 ** 2:.2f} Mb"
    elif num_bytes < 1024**4:
        return f"{num_bytes / 1024 ** 3:.2f} Gb"
    else:
        return f"{num_bytes / 1024 ** 4:.2f} Tb"


def longest_common_prefix(strs):
    if not strs:
        return ""

    # Start with the first string as the prefix
    prefix = strs[0]

    # Compare the prefix with each string in the list
    for string in strs[1:]:
        # Reduce the prefix length until it matches the start of the string
        while string[: len(prefix)] != prefix:
            prefix = prefix[:-1]
            if not prefix:
                return ""

    return prefix


def sleep(seconds):
    if seconds:
        time_slept = 0
        seconds_times_ten = int(seconds * 10)
        for _ in track(range(seconds_times_ten), description="Sleeping"):
            time.sleep(0.01)
            time_slept += 0.01
            if time_slept >= seconds:
                break


def time_to_wait_given_remaining_quota(remaining_quota, duration_to_reset_in_seconds):
    """
    > time_to_wait_given_remaining_quota(0, 3600)
    3600
    > time_to_wait_given_remaining_quota(1, 3600)
    3600
    > time_to_wait_given_remaining_quota(2, 3600)
    1800
    > time_to_wait_given_remaining_quota(3600, 3600)
    1
    > time_to_wait_given_remaining_quota(7200, 3600)
    0.5
    """
    if remaining_quota == 0:
        return duration_to_reset_in_seconds
    return duration_to_reset_in_seconds / remaining_quota


def is_valid_url(url):
    """
    > is_valid_url("https://api.epa.gov/easey/bulk-files")
    True
    > is_valid_url("https://api.epa.gov/easey/bulk-files/")
    True
    > is_valid_url("Bob")
    False
    """
    parsed = urlparse(url)
    if all([parsed.scheme, parsed.netloc]):
        return parsed
    return None


def is_valid_filename(path):
    """
    > is_valid_filename("john")
    False
    > is_valid_filename("john.txt")
    True
    > is_valid_filename("john.txt/")
    False
    > is_valid_filename("/some/dir/john.txt")
    True
    """
    return all(os.path.splitext(os.path.basename(path)))


class DownloadError(Exception):
    pass


class RateLimitState(enum.Enum):
    UNKNOWN = 1
    KNOWN = 2


def find_key_matching(headers, regex):
    for key in headers:
        if regex.fullmatch(key):
            return key
    return None


class RateLimitPair(namedtuple("RateLimitPair", ["n", "state"])):
    def __str__(self):
        return f"{self.n} ({self.state})"


class RateLimit(namedtuple("RateLimit", ["quota", "rate_limit", "retry_after"])):
    def __str__(self):
        return f"Quota: {self.quota}, Rate Limit: {self.rate_limit}, Retry After: {self.retry_after}"


class DowloadResult(
    namedtuple(
        "DownloadResult", ["url", "success", "status_code", "rate_limits", "skip"]
    )
):
    def __str__(self):
        return f"URL: {self.url}, Success: {self.success}, Status Code: {self.status_code}, Rate Limits: {self.rate_limits}, skip: {self.skip}"


def get_quota_remaining(headers):
    """
    > get_quota_remaining({"X-Rate-Limit-Remaining": "100"})
    (100, RateLimitState.KNOWN)
    > get_quota_remaining({"X-Rate-Limit-Remaining": "0"})
    (0, RateLimitState.KNOWN)
    > get_quota_remaining({})
    (0, RateLimitState.UNKNOWN)
    """
    regex = re.compile(r"(X-|)Rate-?Limit-Remaining", re.IGNORECASE)
    key = find_key_matching(headers, regex)
    if key:
        return RateLimitPair(int(headers[key]), RateLimitState.KNOWN)
    return RateLimitPair(0, RateLimitState.UNKNOWN)


def get_rate_limit(headers):
    """
    > get_rate_limit({"X-Rate-Limit-Limit": "100"})
    (100, RateLimitState.KNOWN)
    > get_rate_limit({"X-Rate-Limit-Limit": "0"})
    (0, RateLimitState.KNOWN)
    > get_rate_limit({})
    (0, RateLimitState.UNKNOWN)
    """
    regex = re.compile(r"(X-|)Rate-?Limit-Limit", re.IGNORECASE)
    key = find_key_matching(headers, regex)
    if key:
        return RateLimitPair(int(headers[key]), RateLimitState.KNOWN)
    return RateLimitPair(0, RateLimitState.UNKNOWN)


def get_retry_after(headers):
    """
    > get_retry_after({"Retry-After": "100"})
    (100, RateLimitState.KNOWN)
    > get_retry_after({"Retry-After": "0"})
    (0, RateLimitState.KNOWN)
    > get_retry_after({})
    (0, RateLimitState.UNKNOWN)
    """
    regex = re.compile(r"Retry-?After", re.IGNORECASE)
    key = find_key_matching(headers, regex)
    if key:
        return RateLimitPair(int(headers[key]), RateLimitState.KNOWN)
    return RateLimitPair(0, RateLimitState.UNKNOWN)


def get_rate_limits(headers):
    quota_remaining = get_quota_remaining(headers)
    rate_limit = get_rate_limit(headers)
    retry_after = get_retry_after(headers)
    return RateLimit(quota_remaining, rate_limit, retry_after)


def blank_rate_limits():
    return RateLimit(
        RateLimitPair(0, RateLimitState.UNKNOWN),
        RateLimitPair(0, RateLimitState.UNKNOWN),
        RateLimitPair(0, RateLimitState.UNKNOWN),
    )


class Downloader:
    def __init__(
        self,
        urls,
        download_dir,
        prefixes_to_remove=[],
        max_tries=10,
    ):
        self.urls = urls
        self.download_dir = download_dir
        self.prefixes_to_remove = prefixes_to_remove
        self.last_request_time = None
        self.last_download_time = None
        self.number_of_successful_downloads = 0
        self.number_of_failed_downloads = 0
        self.number_of_existing_files = 0
        self.max_tries = max_tries

    def download_file(self, url):
        url = url.strip()
        parsed = is_valid_url(url)
        if not parsed:
            logger.error(f"Invalid URL: {url}")
            return
        path = parsed.path.lstrip("/")
        old_path = path
        for prefix in self.prefixes_to_remove:
            path = path.replace(prefix.lstrip("/"), "")
        local_path = os.path.join(self.download_dir, path.lstrip("/"))
        logger.debug(
            f"Old path: {old_path} new path: {path}; Local path: {local_path}; URL: {url}; Prefixes: {self.prefixes_to_remove}"
        )
        if not is_valid_filename(local_path):
            logger.error(f"Invalid filename: {local_path}")
            return DowloadResult(url, False, 0, blank_rate_limits(), True)
        if os.path.exists(local_path):
            logger.info(f"{local_path} already exists, skipping.")
            self.number_of_existing_files += 1
            return DowloadResult(url, True, 200, blank_rate_limits(), True)
        # OK, let's try to download the file
        self.last_request_time = time.time()
        r = requests.get(url, stream=True)
        # r.raise_for_status()
        status_code = r.status_code
        rate_limits = get_rate_limits(r.headers)
        logger.debug(f"RATE LIMITS: {rate_limits}")
        success = status_code >= 200 and status_code < 300
        logger.debug(
            f"SUCCESS: {success}; STATUS CODE: {status_code}; URL: <<<<{url}>>>>"
        )
        content_length = r.headers.get("Content-Length")
        sz = "Unknown"
        if content_length:
            sz = humanize_bytes(int(content_length))
        logger.debug(f"Content length: {sz}")
        download_result = DowloadResult(url, success, status_code, rate_limits, False)
        if success:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            # if we fail to write the content, well, let's just fail
            try:
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
            except Exception as e:
                logger.error(f"Error writing to {local_path}: {e}")
                self.number_of_failed_downloads += 1
                return download_result
            logger.info(f"Downloaded {url} to {local_path}; Content size: {sz}")
            self.last_download_time = time.time()
            self.number_of_successful_downloads += 1
        else:
            logger.error(f"Error downloading {url}, result: {download_result}")
            self.number_of_failed_downloads += 1
        return download_result

    def download_all(self):
        number_of_urls = len(self.urls)
        for i, url in enumerate(self.urls, start=1):
            percent_done = 100.0 * i / number_of_urls
            logger.info(
                f"Downloading {i}/{number_of_urls} ({percent_done:.2f}%): {url} ..."
            )
            for attempt_number in range(self.max_tries):
                if attempt_number > 0:
                    logger.info(
                        f"Attempt number {attempt_number + 1} to download {url}"
                    )
                result = self.download_file(url)
                if result.skip:
                    break
                if not result.success:
                    logger.error(f"Failed to download {url}, result: {result}")
                self.maybe_wait(result.rate_limits, result.success, attempt_number + 1)
                if result.success:
                    break

    def maybe_wait(self, rate_limits, success, attempt_number):
        if success:
            if rate_limits.retry_after.n > 0:
                sleep(rate_limits.retry_after.n)
            elif (
                rate_limits.quota.n == 0
                and rate_limits.quota.state == RateLimitState.KNOWN
            ):
                sleep(2**attempt_number)
            elif rate_limits.quota.state == RateLimitState.UNKNOWN:
                pass  # don't know what to do here
            else:
                pass  # don't know what to do here; might be unreachable
            # elif rate_limits.quota.n > 0 and rate_limits.rate_limit.n > 0:
            #     # eg 300 left in a 1000/hr limit, reset time in (300/1000) * 3600 seconds
            #     reset_time = rate_limits.quota.n / rate_limits.rate_limit.n * 3600
            #     time_to_wait = time_to_wait_given_remaining_quota(
            #         rate_limits.quota.n, reset_time
            #     )
            #     logger.info(f"Waiting for {time_to_wait} seconds")
            #     sleep(time_to_wait)
        if not success:
            if rate_limits.retry_after.n > 0:
                logger.info(f"Waiting for {rate_limits.retry_after.n} seconds")
                sleep(rate_limits.retry_after.n)
            else:
                sleep(2**attempt_number)


@click.command()
@click.option(
    "--url-file",
    type=click.Path(exists=True),
    required=True,
    help="Path to a file containing URLs.",
)
@click.option(
    "--download-dir",
    type=click.Path(file_okay=False, dir_okay=True),
    default="/tmp/data",
    help="Directory to save downloads.",
)
@click.option(
    "--prefixes-to-remove",
    multiple=True,
    help="Prefixes to remove from the URL path when saving the file.",
)
@click.option(
    "--auto-remove-prefix",
    is_flag=True,
    help="Remove the longest common prefix from the URL paths",
)
@click.option(
    "--randomize",
    is_flag=True,
    help="Randomize the order of the URLs",
)
@click.option(
    "--log-level",
    default="INFO",
    help="Logging level.",
)
@click.option(
    "--max-tries",
    default=10,
    help="Maximum number of retries on request failures",
)
def cli(
    url_file,
    download_dir,
    prefixes_to_remove,
    auto_remove_prefix,
    randomize,
    log_level,
    max_tries,
):
    logger.add(sys.stdout, level=log_level.upper())
    prefixes_to_remove = list(prefixes_to_remove)
    with open(url_file, "r") as f:
        urls = [url.strip() for url in f.readlines()]
    if randomize:
        random.shuffle(urls)
    if auto_remove_prefix:
        longest_prefix = longest_common_prefix(
            [urlparse(url).path for url in urls if url]
        )
        prefixes_to_remove.append(longest_prefix)
        logger.info(f"Auto-removing prefix: {longest_prefix}")
    downloader = Downloader(urls, download_dir, prefixes_to_remove, max_tries=max_tries)
    downloader.download_all()
    logger.info(f"Number of existing files: {downloader.number_of_existing_files}")
    logger.info(
        f"Number of successful downloads: {downloader.number_of_successful_downloads}"
    )
    logger.info(f"Number of failed downloads: {downloader.number_of_failed_downloads}")


if __name__ == "__main__":
    cli()
