"""Test dedup behaviour with --set-exif-datetime for EXIF-injected JPEGs.

When --set-exif-datetime is active, piexif.insert() replaces the entire EXIF
APP1 segment, changing the file size unpredictably. The size invariant is broken
by design, so size-based dedup must be skipped for JPEGs.
"""

import inspect
import os
from typing import Any, List, Tuple
from unittest import TestCase, mock

import pytest
from requests import Response

from pyicloud_ipd.services.photos import PhotoAsset
from tests.helpers import (
    path_from_project_root,
    run_icloudpd_test,
)


class DedupExifSkipTest(TestCase):
    """Dedup should be skipped for JPEGs when --set-exif-datetime is active."""

    @pytest.fixture(autouse=True)
    def inject_fixtures(self) -> None:
        self.root_path = path_from_project_root(__file__)
        self.fixtures_path = os.path.join(self.root_path, "fixtures")

    def test_jpeg_with_exif_datetime_skips_size_dedup(self) -> None:
        """A JPEG with mismatched size should NOT trigger dedup when --set-exif-datetime is on.

        Simulates a file that was previously EXIF-injected (size differs from iCloud).
        The cassette reports IMG_7409.JPG as 1884695 bytes. We create a local file
        with a different size. With --set-exif-datetime, this should be treated as
        the same file (skip dedup), not as a different file.
        """
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])

        # Create all files as existing so no downloads are attempted.
        # IMG_7409.JPG has a size mismatch to test the EXIF skip.
        # Use --recent 3 to limit to IMG_7409 (JPG+MOV) and IMG_7408 (JPG+MOV+skipped videos)
        files_to_create = [
            ("2018/07/31", "IMG_7409.JPG", 1884780),  # +85 bytes (typical EXIF delta)
            ("2018/07/31", "IMG_7409.MOV", 3294075),   # Correct size
            ("2018/07/30", "IMG_7408.JPG", 1151066),   # Correct size
            ("2018/07/30", "IMG_7408.MOV", 1606512),   # Correct size
        ]

        # Nothing should be downloaded
        files_to_download: List[Tuple[str, str]] = []

        with mock.patch("icloudpd.exif_datetime.get_photo_exif") as get_exif_patched:
            get_exif_patched.return_value = "2018:07:31 07:22:24"
            data_dir, result = run_icloudpd_test(
                self.assertEqual,
                self.root_path,
                base_dir,
                "listing_photos.yml",
                files_to_create,
                files_to_download,
                [
                    "--username",
                    "jdoe@gmail.com",
                    "--password",
                    "password1",
                    "--recent",
                    "3",
                    "--skip-videos",
                    "--set-exif-datetime",
                    "--no-progress-bar",
                    "--threads-num",
                    "1",
                ],
            )

            # Should NOT see dedup message for IMG_7409.JPG
            self.assertNotIn("deduplicated", result.output)
            # Should see EXIF skip message
            self.assertIn("EXIF-injected, skipping size dedup", result.output)
            # No dedup-suffix file should exist
            dedup_files = [
                f
                for f in os.listdir(os.path.join(data_dir, "2018", "07", "31"))
                if "-1884695" in f
            ]
            self.assertEqual(dedup_files, [], "No dedup-suffix files should be created")

    def test_non_jpeg_with_exif_datetime_still_dedupes(self) -> None:
        """A MOV file with mismatched size should still trigger dedup even with --set-exif-datetime.

        EXIF injection only applies to JPEGs, so MOV files should still use size-based dedup.
        """
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])

        # Create IMG_7409.MOV with wrong size (should trigger dedup for MOV)
        files_to_create = [
            ("2018/07/31", "IMG_7409.JPG", 1884695),   # Correct size
            ("2018/07/31", "IMG_7409.MOV", 1),          # Wrong size → dedup
            ("2018/07/30", "IMG_7408.JPG", 1151066),
            ("2018/07/30", "IMG_7408.MOV", 1606512),
        ]

        # --recent 3 returns 3 photos which includes IMG_7407
        files_to_download: List[Tuple[str, str]] = [
            ("2018/07/31", "IMG_7409-3294075.MOV"),  # Dedup copy
            ("2018/07/30", "IMG_7407.JPG"),
            ("2018/07/30", "IMG_7407.MOV"),
        ]

        orig_download = PhotoAsset.download

        def mocked_download(self: PhotoAsset, session: Any, _url: str, start: int) -> Response:
            if not hasattr(PhotoAsset, "already_downloaded"):
                response = orig_download(self, session, _url, start)
                setattr(PhotoAsset, "already_downloaded", True)  # noqa: B010
                return response
            return mock.MagicMock()

        with mock.patch.object(PhotoAsset, "download", new=mocked_download):
            with mock.patch("icloudpd.exif_datetime.get_photo_exif") as get_exif_patched:
                get_exif_patched.return_value = "2018:07:31 07:22:24"
                data_dir, result = run_icloudpd_test(
                    self.assertEqual,
                    self.root_path,
                    base_dir,
                    "listing_photos.yml",
                    files_to_create,
                    files_to_download,
                    [
                        "--username",
                        "jdoe@gmail.com",
                        "--password",
                        "password1",
                        "--recent",
                        "3",
                        "--skip-videos",
                        "--set-exif-datetime",
                        "--no-progress-bar",
                        "--threads-num",
                        "1",
                    ],
                )

                # MOV should still be deduplicated
                self.assertIn("deduplicated", result.output)

    def test_jpeg_without_exif_datetime_still_dedupes(self) -> None:
        """Without --set-exif-datetime, JPEG size mismatches should trigger dedup as normal."""
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])

        files_to_create = [
            ("2018/07/31", "IMG_7409.JPG", 1),          # Wrong size → dedup
            ("2018/07/31", "IMG_7409.MOV", 3294075),     # Correct size
            ("2018/07/30", "IMG_7408.JPG", 1151066),
            ("2018/07/30", "IMG_7408.MOV", 1606512),
        ]

        files_to_download: List[Tuple[str, str]] = [
            ("2018/07/31", "IMG_7409-1884695.JPG"),  # Dedup copy
            ("2018/07/30", "IMG_7407.JPG"),
            ("2018/07/30", "IMG_7407.MOV"),
        ]

        orig_download = PhotoAsset.download

        def mocked_download(self: PhotoAsset, session: Any, _url: str, start: int) -> Response:
            if not hasattr(PhotoAsset, "already_downloaded"):
                response = orig_download(self, session, _url, start)
                setattr(PhotoAsset, "already_downloaded", True)  # noqa: B010
                return response
            return mock.MagicMock()

        with mock.patch.object(PhotoAsset, "download", new=mocked_download):
            data_dir, result = run_icloudpd_test(
                self.assertEqual,
                self.root_path,
                base_dir,
                "listing_photos.yml",
                files_to_create,
                files_to_download,
                [
                    "--username",
                    "jdoe@gmail.com",
                    "--password",
                    "password1",
                    "--recent",
                    "3",
                    "--skip-videos",
                    # NO --set-exif-datetime
                    "--no-progress-bar",
                    "--threads-num",
                    "1",
                ],
            )

            # JPEG should be deduplicated (no EXIF skip)
            self.assertIn("deduplicated", result.output)
            self.assertNotIn("EXIF-injected", result.output)
