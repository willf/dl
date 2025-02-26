# bulk download

Attempt to bulk download a list of URLs with some tenacity, but also
some grace. Attempts to honor the server's rate limiting and retries
on various failures.

```
$ uv run dl.py --help
Usage: dl.py [OPTIONS]

Options:
  --url-file PATH            Path to a file containing URLs.  [required]
  --download-dir PATH        Directory to save downloads.
  --prefixes-to-remove TEXT  Prefixes to remove from the URL path when saving
                             the file.
  --auto-remove-prefix       Remove the longest common prefix from the URL
                             paths
  --regex TEXT               Regular expression to match URLs to download.
  --reverse                  Reverse the regex match, i.e., download URLs that
                             do not match the regex.
  --randomize                Randomize the order of the URLs
  --log-level TEXT           Logging level.
  --max-tries INTEGER        Maximum number of retries on request failures
  --version                  Show the version and exit.
  --log-file PATH            Path to a file to save logs.
  --dry-run                  If set, do not actually download the files, just
                             log what would be done.
  --help                     Show this message and exit.
```

Example:

```
uv run dl.py --url-file urls.txt --download-dir downloads --auto-remove-prefix
```
