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
from dataclasses import dataclass


logger.remove(0)

MAX_WAIT_TIME = 3600  # seconds, 1 hour
CONNECTION_ERROR = -1  # magic number for connection error


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


@dataclass
class RateLimitPair:
    n: int
    state: RateLimitState


@dataclass
class RateLimits:
    remaining: RateLimitPair
    rate_limit: RateLimitPair
    retry_after: RateLimitPair
    reset_after: RateLimitPair


@dataclass
class DownloadResult:
    url: str
    success: bool
    status_code: int
    rate_limits: RateLimits
    skip: bool
    attempt_number: int = 0

    def wait_time_policy(self):
        # if for some reason we are skipping this item, we do not need to wait
        if self.skip:
            return 0
        # if we have a retry-after header, we should wait that amount of time
        # but perhaps not more than the MAX_WAIT_TIME
        if (
            self.rate_limits.retry_after.n > 0
            and self.rate_limits.retry_after.state == RateLimitState.KNOWN
        ):
            return min(self.rate_limits.retry_after.n, MAX_WAIT_TIME)
        ## If we have both a RateLimitRemaining and RateLimitReset header, we
        ## can calculate how long to wait. But we need to check if the
        ## RateLimitReset is a Unix epoch time or a duration in seconds
        if (
            self.rate_limits.remaining.n > 0
            and self.rate_limits.remaining.state == RateLimitState.KNOWN
            and self.rate_limits.reset_after.n > 0
            and self.rate_limits.reset_after.state == RateLimitState.KNOWN
        ):
            if self.rate_limits.reset_after.n > 1000000000:
                duration = self.rate_limits.reset_after.n - time.time
            else:
                duration = self.rate_limits.reset_after.n
            # we can only do n calls in duration seconds, so we should wait
            # duration / n seconds
            return duration / self.rate_limits.remaining.n
        ## if the status is 429, a server problem, or a connection problem
        ## we should wait 2^attempt_number seconds
        if self.status_code in [429, 503, CONNECTION_ERROR]:
            logger.info(
                f"Status code: {self.status_code}; attempt number: {self.attempt_number}"
            )
            return 2**self.attempt_number
        ## if we don't know what to do, and we are at attempt number > 0, we
        ## should wait 2^attempt_number seconds
        if self.attempt_number > 0:
            return 2**self.attempt_number
        ## if we know *nothing* then don't wait
        return 0


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


def get_ratelimit_reset(headers):
    """
    > get_ratelimit_reset({"X-Rate-Limit-Reset": "100"})
    (100, RateLimitState.KNOWN)
    > get_ratelimit_reset({"X-Rate-Limit-Reset": "0"})
    (0, RateLimitState.KNOWN)
    > get_ratelimit_reset({})
    (0, RateLimitState.UNKNOWN)
    """
    regex = re.compile(r"(X-|)Rate-?Limit-Reset", re.IGNORECASE)
    key = find_key_matching(headers, regex)
    if key:
        return RateLimitPair(int(headers[key]), RateLimitState.KNOWN)
    return RateLimitPair(0, RateLimitState.UNKNOWN)


def get_rate_limits(headers):
    quota_remaining = get_quota_remaining(headers)
    rate_limit = get_rate_limit(headers)
    retry_after = get_retry_after(headers)
    reset_after = get_ratelimit_reset(headers)
    return RateLimits(quota_remaining, rate_limit, retry_after, reset_after)


def blank_rate_limits():
    return RateLimits(
        RateLimitPair(0, RateLimitState.UNKNOWN),
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

    def download_file(self, url, attempt_number):
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
            return DownloadResult(
                url, False, 0, blank_rate_limits(), True, attempt_number
            )
        if os.path.exists(local_path):
            logger.info(f"{local_path} already exists, skipping.")
            self.number_of_existing_files += 1
            return DownloadResult(
                url, True, 200, blank_rate_limits(), True, attempt_number
            )
        # OK, let's try to download the file
        self.last_request_time = time.time()
        try:
            r = requests.get(url, stream=True)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            logger.error(f"Request failed: {e}")
            self.number_of_failed_downloads += 1
            return DownloadResult(
                url, False, CONNECTION_ERROR, blank_rate_limits(), False, attempt_number
            )

        status_code = r.status_code
        logger.trace(f"Headers: {r.headers}")
        rate_limits = get_rate_limits(r.headers)
        logger.debug(f"RATE LIMITS: {rate_limits}")
        success = status_code >= 200 and status_code < 300
        logger.debug(f"SUCCESS: {success}; STATUS CODE: {status_code}; URL: {url}")
        content_length = r.headers.get("Content-Length")
        sz = "Unknown"
        if content_length:
            sz = humanize_bytes(int(content_length))
        logger.debug(f"Content length: {sz}")
        download_result = DownloadResult(
            url, success, status_code, rate_limits, False, attempt_number
        )
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
            result = None
            for attempt_number in range(self.max_tries):
                if attempt_number > 0:
                    logger.info(
                        f"Attempt number {attempt_number + 1} to download {url}"
                    )
                result = self.download_file(url, attempt_number + 1)
                sleep_time = result.wait_time_policy()
                if sleep_time > 0:
                    sleep(sleep_time)
                if result.success or result.skip:
                    break
            if not (result or result.success) and not result.skip:
                logger.error(
                    f"Failed to download {url} after {self.max_tries} attempts"
                )


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
    logger.info(f"Download complete; processed {len(urls)} URLs")
    logger.info(f"Number of existing files: {downloader.number_of_existing_files}")
    logger.info(
        f"Number of successful downloads: {downloader.number_of_successful_downloads}"
    )
    logger.info(
        f"Number of failed download attempts: {downloader.number_of_failed_downloads}"
    )


if __name__ == "__main__":
    cli()
