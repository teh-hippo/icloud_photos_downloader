"""Handles file downloads with retries and error handling"""

import base64
import contextlib
import datetime
import logging
import os
import time
from functools import partial
from typing import Callable

from requests import Response
from tzlocal import get_localzone

# Import the constants object so that we can mock WAIT_SECONDS in tests
from icloudpd import constants
from pyicloud_ipd.asset_version import AssetVersion, calculate_version_filename
from pyicloud_ipd.base import PyiCloudService
from pyicloud_ipd.exceptions import PyiCloudAPIResponseException
from pyicloud_ipd.raw_policy import RawTreatmentPolicy
from pyicloud_ipd.services.photos import PhotoAsset, PhotoAssetRefreshError
from pyicloud_ipd.version_size import VersionSize


def update_mtime(created: datetime.datetime, download_path: str) -> None:
    """Set the modification time of the downloaded file to the photo creation date"""
    if created:
        created_date = None
        try:
            created_date = created.astimezone(get_localzone())
        except (ValueError, OSError):
            # We already show the timezone conversion error in base.py,
            # when generating the download directory.
            # So just return silently without touching the mtime.
            return
        set_utime(download_path, created_date)


def set_utime(download_path: str, created_date: datetime.datetime) -> None:
    """Set date & time of the file"""
    try:
        ctime = time.mktime(created_date.timetuple())
    except OverflowError:
        ctime = time.mktime(datetime.datetime(1970, 1, 1, 0, 0, 0).timetuple())
    os.utime(download_path, (ctime, ctime))


def mkdirs_for_path(logger: logging.Logger, download_path: str) -> bool:
    """Creates hierarchy of folders for file path if it needed"""
    try:
        # get back the directory for the file to be downloaded and create it if
        # not there already
        download_dir = os.path.dirname(download_path)
        os.makedirs(name=download_dir, exist_ok=True)
        return True
    except OSError:
        logger.error(
            "Could not create folder %s",
            download_dir,
        )
        return False


def mkdirs_for_path_dry_run(logger: logging.Logger, download_path: str) -> bool:
    """DRY Run for Creating hierarchy of folders for file path"""
    download_dir = os.path.dirname(download_path)
    if not os.path.exists(download_dir):
        logger.debug(
            "[DRY RUN] Would create folder hierarchy %s",
            download_dir,
        )
    return True


def download_response_to_path(
    response: Response,
    temp_download_path: str,
    append_mode: bool,
    download_path: str,
    created_date: datetime.datetime,
    expected_size: int = 0,
) -> bool:
    """Saves response content into file with desired created date"""
    with open(temp_download_path, ("ab" if append_mode else "wb")) as file_obj:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                file_obj.write(chunk)
    # Verify downloaded size matches expected before atomic rename
    if expected_size > 0:
        actual_size = os.path.getsize(temp_download_path)
        if actual_size != expected_size:
            logger = logging.getLogger(__name__)
            logger.warning(
                "Download size mismatch for %s: got %d bytes, expected %d",
                download_path,
                actual_size,
                expected_size,
            )
    os.rename(temp_download_path, download_path)
    update_mtime(created_date, download_path)
    return True


def download_response_to_path_dry_run(
    logger: logging.Logger,
    _response: Response,
    _temp_download_path: str,
    _append_mode: bool,
    download_path: str,
    _created_date: datetime.datetime,
) -> bool:
    """Pretends to save response content into a file with desired created date"""
    logger.info(
        "[DRY RUN] Would download %s",
        download_path,
    )
    return True


def download_media(
    logger: logging.Logger,
    dry_run: bool,
    icloud: PyiCloudService,
    photo: PhotoAsset,
    download_path: str,
    version: AssetVersion,
    size: VersionSize,
    filename_builder: Callable[[PhotoAsset], str],
    raw_policy: RawTreatmentPolicy,
) -> bool:
    """Download the photo to path, with layered retry on CloudKit URL expiry.

    Retry strategy when the CDN returns 410 Gone (signed URL aged out):
    1. Retry the same URL up to ``SAME_URL_RETRIES`` times (cheap; covers
       transient blips).
    2. Call ``photo.refresh()`` to re-fetch the asset's records and obtain
       freshly-signed URLs, then retry up to ``REFRESHED_URL_RETRIES`` times.
    3. If still failing, warn and skip; the next backup run will pick it up.

    A ``MAX_TOTAL_ATTEMPTS`` cap covers every path (same-URL retries,
    refresh retries, session re-auth) to prevent runaway loops.
    """

    mkdirs_local = mkdirs_for_path_dry_run if dry_run else mkdirs_for_path
    if not mkdirs_local(logger, download_path):
        return False

    def derive_local_state(
        current_version: AssetVersion,
    ) -> tuple[str, Callable[..., bool]]:
        """Compute the .part path and download_local closure for a version.

        Must be re-called after :py:meth:`PhotoAsset.refresh` since a refresh
        may (rarely) change the asset's checksum / expected size.
        """
        checksum_bytes = base64.b64decode(current_version.checksum)
        checksum32 = base64.b32encode(checksum_bytes).decode()
        local_dir = os.path.dirname(download_path)
        temp = os.path.join(local_dir, checksum32) + ".part"
        local: Callable[..., bool] = (
            partial(download_response_to_path_dry_run, logger)
            if dry_run
            else partial(download_response_to_path, expected_size=current_version.size)
        )
        return temp, local

    temp_download_path, download_local = derive_local_state(version)

    same_url_attempts = 0
    refresh_attempts = 0
    auth_attempts = 0
    total_attempts = 0
    refreshed = False
    error_filename = filename_builder(photo)

    while total_attempts < constants.MAX_TOTAL_ATTEMPTS:
        total_attempts += 1
        try:
            append_mode = os.path.exists(temp_download_path)
            current_size = os.path.getsize(temp_download_path) if append_mode else 0
            if append_mode:
                logger.debug("Resuming downloading of %s from %d", download_path, current_size)

            photo_response = photo.download(icloud.photos.session, version.url, current_size)
            if photo_response.ok:
                return download_local(
                    photo_response,
                    temp_download_path,
                    append_mode,
                    download_path,
                    photo.created,
                )

            # Non-OK response without an exception (rare path — used to mean
            # "no URL for this size"). Preserve the original log line.
            from icloudpd.base import lp_filename_original as simple_lp_filename_generator

            base_filename = filename_builder(photo)
            version_filename = calculate_version_filename(
                base_filename, version, size, simple_lp_filename_generator, photo.item_type
            )
            logger.error(
                "Could not find URL to download %s for size %s",
                version_filename,
                size.value,
            )
            return False

        except PyiCloudAPIResponseException as ex:
            code = str(getattr(ex, "code", ""))
            is_auth_err = "Invalid global session" in str(ex)
            is_gone = code == "410" or "Gone" in str(ex)

            if is_auth_err:
                # Match historical behaviour: re-authenticate once per
                # download_media call. If the next attempt still hits an
                # auth error, give up. The terminal "Could not download"
                # log line is asserted by the test suite.
                if auth_attempts >= 1:
                    logger.error(
                        "Could not download %s. Please try again later.",
                        error_filename,
                    )
                    return False
                logger.error("Session error, re-authenticating...")
                icloud.authenticate()
                auth_attempts += 1
                continue

            if is_gone and not refreshed:
                if same_url_attempts < constants.SAME_URL_RETRIES:
                    same_url_attempts += 1
                    wait_time = same_url_attempts * constants.WAIT_SECONDS
                    logger.debug(
                        "410 Gone for %s, same-URL retry %d/%d after %ds",
                        error_filename,
                        same_url_attempts,
                        constants.SAME_URL_RETRIES,
                        wait_time,
                    )
                    time.sleep(wait_time)
                    continue

                # Same-URL budget exhausted → refetch the asset's records
                # and pick up freshly-signed URLs.
                try:
                    logger.debug(
                        "Refreshing URLs for %s after repeated 410 Gone",
                        error_filename,
                    )
                    photo.refresh()
                except PhotoAssetRefreshError as refresh_err:
                    logger.warning(
                        "Skipping %s: URL expired and refresh failed (%s)",
                        error_filename,
                        refresh_err,
                    )
                    return False
                except PyiCloudAPIResponseException as refresh_api_err:
                    logger.warning(
                        "Skipping %s: URL expired and refresh hit API error (%s)",
                        error_filename,
                        refresh_api_err,
                    )
                    return False

                fresh_versions = photo.versions_with_raw_policy(raw_policy)
                new_version = fresh_versions.get(size)
                if new_version is None:
                    logger.warning(
                        "Skipping %s: size %s no longer available after refresh",
                        error_filename,
                        size.value,
                    )
                    return False
                if new_version.checksum != version.checksum:
                    # Different checksum means the asset itself changed
                    # server-side; the existing .part is no longer valid.
                    logger.warning(
                        "Checksum for %s changed after refresh; discarding partial download",
                        error_filename,
                    )
                    if os.path.exists(temp_download_path):
                        with contextlib.suppress(OSError):
                            os.remove(temp_download_path)
                version = new_version
                temp_download_path, download_local = derive_local_state(version)
                refreshed = True
                refresh_attempts = 0
                continue

            if is_gone and refreshed:
                if refresh_attempts < constants.REFRESHED_URL_RETRIES:
                    refresh_attempts += 1
                    wait_time = refresh_attempts * constants.WAIT_SECONDS
                    logger.debug(
                        "410 Gone after refresh for %s, retry %d/%d after %ds",
                        error_filename,
                        refresh_attempts,
                        constants.REFRESHED_URL_RETRIES,
                        wait_time,
                    )
                    time.sleep(wait_time)
                    continue
                logger.warning(
                    "Skipping %s after refresh + retries still returned 410 Gone",
                    error_filename,
                )
                return False

            # Other API errors (not auth, not 410): preserve the historical
            # "no retries on non-410 errors" behaviour. The new layered retry
            # exists specifically for URL-expiry; broadening it to all API
            # errors would change behaviour for unrelated failure modes.
            logger.error(
                "Could not download %s. Please try again later.",
                error_filename,
            )
            return False

        except OSError:
            logger.error(
                "IOError while writing file to %s. "
                "You might have run out of disk space, or the file "
                "might be too large for your OS. "
                "Skipping this file...",
                download_path,
            )
            return False

    logger.error(
        "Could not download %s after %d attempts. Please try again later.",
        error_filename,
        total_attempts,
    )
    return False
