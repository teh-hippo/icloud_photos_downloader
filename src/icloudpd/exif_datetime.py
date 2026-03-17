"""Get/set EXIF dates from photos"""

import logging
import typing

import piexif
from piexif._exceptions import InvalidImageDataError


def get_photo_exif(logger: logging.Logger, path: str) -> str | None:
    """Get EXIF date for a photo, return nothing if there is an error"""
    try:
        exif_dict: piexif.ExifIFD = piexif.load(path)
        exif_ifd = exif_dict.get("Exif")
        if exif_ifd is None:
            return None
        value = exif_ifd.get(36867)
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return typing.cast(str | None, value)
    except (ValueError, InvalidImageDataError, AttributeError, TypeError, UnicodeDecodeError):
        logger.debug("Error fetching EXIF data for %s", path)
        return None


def set_photo_exif(logger: logging.Logger, path: str, date: str) -> None:
    """Set EXIF date on a photo, do nothing if there is an error"""
    try:
        exif_dict = piexif.load(path)
        first_ifd = exif_dict.get("1st")
        exif_ifd = exif_dict.get("Exif")
        if first_ifd is None or exif_ifd is None:
            logger.debug("Missing EXIF IFD for %s, skipping", path)
            return
        first_ifd[306] = date
        exif_ifd[36867] = date
        exif_ifd[36868] = date
        exif_bytes = piexif.dump(exif_dict)
        piexif.insert(exif_bytes, path)
    except (ValueError, InvalidImageDataError, AttributeError, TypeError, KeyError):
        logger.debug("Error setting EXIF data for %s", path)
        return
