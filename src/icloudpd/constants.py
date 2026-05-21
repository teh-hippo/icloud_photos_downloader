"""Constants"""

from typing import Final

# For retrying connection after timeouts and errors
MAX_RETRIES: Final[int] = 0
WAIT_SECONDS: Final[int] = 5

# CloudKit asset URLs are signed at page-fetch time and expire after the page
# ages out (~15 min observed). When a 410 Gone is returned we retry once with
# the same URL (cheap, catches transient CDN blips), then refetch the asset's
# record to get a freshly-signed URL and retry that. After exhausting both,
# the file is skipped with a warning so the next run can pick it up.
SAME_URL_RETRIES: Final[int] = 1
REFRESHED_URL_RETRIES: Final[int] = 1

# Hard upper bound on total attempts in the download retry loop, covering
# every path (same-URL retries, refresh retries, session re-auth). Prevents
# pathological loops if a category of error keeps reasserting itself.
MAX_TOTAL_ATTEMPTS: Final[int] = 10
