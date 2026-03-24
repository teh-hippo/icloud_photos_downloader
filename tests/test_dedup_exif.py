"""Test manifest-based identity matching replaces fragile size-based dedup.

With the asset manifest, file identity is determined by iCloud's recordName
(asset_id), not by comparing file sizes. This eliminates the EXIF re-serialisation
false dedup problem entirely.
"""

import inspect
import os
from typing import List, Tuple
from unittest import TestCase, mock

import pytest

from tests.helpers import (
    path_from_project_root,
    run_icloudpd_test,
)


class ManifestDedupTest(TestCase):
    """Manifest-based sync should track files by identity, not size."""

    @pytest.fixture(autouse=True)
    def inject_fixtures(self) -> None:
        self.root_path = path_from_project_root(__file__)
        self.fixtures_path = os.path.join(self.root_path, "fixtures")

    def test_mismatched_size_jpeg_adopted_by_manifest(self) -> None:
        """A JPEG with mismatched size should be adopted by manifest, not deduped."""
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])

        files_to_create = [
            ("2018/07/31", "IMG_7409.JPG", 1884780),  # +85 bytes (EXIF delta)
            ("2018/07/31", "IMG_7409.MOV", 3294075),
            ("2018/07/30", "IMG_7408.JPG", 1151066),
            ("2018/07/30", "IMG_7408.MOV", 1606512),
        ]
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

            self.assertNotIn("deduplicated", result.output)
            self.assertIn("adopted into manifest", result.output)
            dedup_files = [
                f
                for f in os.listdir(os.path.join(data_dir, "2018", "07", "31"))
                if "-1884695" in f
            ]
            self.assertEqual(dedup_files, [], "No dedup-suffix files should be created")
            self.assertTrue(os.path.isfile(os.path.join(data_dir, ".icloudpd.db")))
