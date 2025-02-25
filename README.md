# bulk download

Attempt to bulk download a list of URLs with some tenacity, but also
some grace. Attempts to honor the server's rate limiting and retries
on various failures.

```
Usage: dl.py [OPTIONS]

Options:
--url-file PATH Path to a file containing URLs. [required]
--download-dir PATH Directory to save downloads.
--prefixes-to-remove TEXT Prefixes to remove from the URL path when saving
the file.
--auto-remove-prefix Remove the longest common prefix from the URL
paths
--randomize Randomize the order of the URLs
--log-level TEXT Logging level.
--max-tries INTEGER Maximum number of retries on request failures
--help Show this message and exit.
```

Example:

```
uv run dl.py --url-file urls.txt --download-dir downloads --auto-remove-prefix
```
