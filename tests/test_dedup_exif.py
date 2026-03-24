"""Test manifest-based identity matching replaces fragile size-based dedup.

With the asset manifest, file identity is determined by iCloud's recordName
(asset_id), not by comparing file sizes. This eliminates the EXIF re-serialisation
false dedup problem entirely.
"""

import inspect
import os
import sqlite3
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

    def test_version_change_triggers_redownload(self) -> None:
        """When manifest has a different version_size, file should be re-downloaded."""
        import shutil
        from tests.helpers import (
            calc_cookie_dir, calc_data_dir, calc_vcr_dir, recreate_path,
            create_files, run_main_env, run_with_cassette, print_result_exception,
            DEFAULT_ENV,
        )
        from foundation.core import compose, partial_2_1
        from functools import partial

        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])
        cookie_dir = calc_cookie_dir(base_dir)
        data_dir = calc_data_dir(base_dir)
        vcr_path = calc_vcr_dir(self.root_path)
        cookie_master_path = calc_cookie_dir(self.root_path)

        for d in [base_dir, data_dir]:
            recreate_path(d)
        shutil.copytree(cookie_master_path, cookie_dir)

        files_to_create = [
            ("2018/07/31", "IMG_7409.JPG", 1884695),
            ("2018/07/31", "IMG_7409.MOV", 3294075),
            ("2018/07/30", "IMG_7408.JPG", 1151066),
            ("2018/07/30", "IMG_7408.MOV", 1606512),
        ]
        create_files(data_dir, files_to_create)

        # Seed manifest with WRONG version_size for IMG_7409.JPG — triggers re-download
        db_path = os.path.join(data_dir, ".icloudpd.db")
        conn = sqlite3.connect(db_path)
        conn.executescript("""\
            CREATE TABLE manifest (
                asset_id TEXT NOT NULL,
                zone_id TEXT NOT NULL DEFAULT '',
                local_path TEXT NOT NULL,
                version_size INTEGER NOT NULL,
                version_checksum TEXT,
                change_tag TEXT,
                downloaded_at TEXT NOT NULL,
                PRIMARY KEY (asset_id, zone_id, local_path)
            );
        """)
        conn.execute(
            "INSERT INTO manifest VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("AY6c+BsE0jjaXx9tmVGJM1D2VcEO", "PrimarySync",
             os.path.join("2018", "07", "31", "IMG_7409.JPG"),
             999, None, None, "2026-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

        combined_env = {**DEFAULT_ENV}
        main_runner = compose(print_result_exception, partial(run_main_env, combined_env, input=None))
        with_cassette_main_runner = partial_2_1(
            run_with_cassette, os.path.join(vcr_path, "listing_photos.yml"), main_runner
        )

        with mock.patch("icloudpd.exif_datetime.get_photo_exif") as get_exif_patched:
            get_exif_patched.return_value = "2018:07:31 07:22:24"
            result = with_cassette_main_runner([
                "-d", data_dir,
                "--cookie-directory", cookie_dir,
                "--username", "jdoe@gmail.com",
                "--password", "password1",
                "--recent", "3",
                "--skip-videos",
                "--set-exif-datetime",
                "--no-progress-bar",
                "--threads-num", "1",
            ])

        self.assertIn("version changed", result.output)

        # Verify manifest was updated with the correct new size
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT version_size FROM manifest WHERE asset_id = ?",
            ("AY6c+BsE0jjaXx9tmVGJM1D2VcEO",),
        ).fetchone()
        conn.close()
        assert row is not None, "Manifest row should exist after re-download"
        self.assertNotEqual(row[0], 999, "version_size should be updated after re-download")
