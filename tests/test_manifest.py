"""Unit tests for icloudpd.manifest — SQLite asset manifest."""

import os
import shutil
import tempfile
from unittest import TestCase

from icloudpd.manifest import ManifestDB, ManifestRow


class TestManifestDB(TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp()
        self._db = ManifestDB(self._tmpdir)
        self._db.open()

    def tearDown(self) -> None:
        self._db.close()
        shutil.rmtree(self._tmpdir)

    def test_db_created_at_expected_path(self) -> None:
        self.assertTrue(os.path.isfile(os.path.join(self._tmpdir, ".icloudpd.db")))

    def test_empty_db_has_zero_count(self) -> None:
        self.assertEqual(self._db.count(), 0)

    def test_upsert_and_lookup(self) -> None:
        self._db.upsert(
            asset_id="ABC123",
            zone_id="PrimarySync",
            local_path="2024/01/IMG_0001.JPG",
            version_size=1884695,
            version_checksum="chk123",
            change_tag="49lb",
        )
        row = self._db.lookup("ABC123", "PrimarySync", "2024/01/IMG_0001.JPG")
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.asset_id, "ABC123")
        self.assertEqual(row.zone_id, "PrimarySync")
        self.assertEqual(row.local_path, "2024/01/IMG_0001.JPG")
        self.assertEqual(row.version_size, 1884695)
        self.assertEqual(row.version_checksum, "chk123")
        self.assertEqual(row.change_tag, "49lb")
        self.assertIsNotNone(row.downloaded_at)

    def test_lookup_missing_returns_none(self) -> None:
        row = self._db.lookup("MISSING", "PrimarySync", "nonexistent.jpg")
        self.assertIsNone(row)

    def test_upsert_updates_existing_row(self) -> None:
        self._db.upsert("ABC", "z", "a.jpg", 100, "c1", "t1")
        self._db.upsert("ABC", "z", "a.jpg", 200, "c2", "t2")
        self.assertEqual(self._db.count(), 1)
        row = self._db.lookup("ABC", "z", "a.jpg")
        assert row is not None
        self.assertEqual(row.version_size, 200)
        self.assertEqual(row.change_tag, "t2")

    def test_same_asset_different_paths(self) -> None:
        """Live photo: one asset produces JPEG + MOV with different local_paths."""
        self._db.upsert("LIVE1", "z", "2024/01/IMG_0001.JPG", 1000)
        self._db.upsert("LIVE1", "z", "2024/01/IMG_0001.MOV", 5000)
        self.assertEqual(self._db.count(), 2)
        jpg = self._db.lookup("LIVE1", "z", "2024/01/IMG_0001.JPG")
        mov = self._db.lookup("LIVE1", "z", "2024/01/IMG_0001.MOV")
        assert jpg is not None and mov is not None
        self.assertEqual(jpg.version_size, 1000)
        self.assertEqual(mov.version_size, 5000)

    def test_same_asset_different_zones(self) -> None:
        """Same recordName in personal and shared library."""
        self._db.upsert("DUP1", "PrimarySync", "a.jpg", 100)
        self._db.upsert("DUP1", "SharedSync-XYZ", "a.jpg", 100)
        self.assertEqual(self._db.count(), 2)

    def test_lookup_by_path(self) -> None:
        self._db.upsert("ABC", "z", "2024/01/IMG_0001.JPG", 1000)
        row = self._db.lookup_by_path("2024/01/IMG_0001.JPG")
        assert row is not None
        self.assertEqual(row.asset_id, "ABC")

    def test_lookup_by_path_missing(self) -> None:
        self.assertIsNone(self._db.lookup_by_path("nonexistent.jpg"))

    def test_remove(self) -> None:
        self._db.upsert("ABC", "z", "a.jpg", 100)
        self.assertEqual(self._db.count(), 1)
        self._db.remove("ABC", "z", "a.jpg")
        self.assertEqual(self._db.count(), 0)

    def test_remove_nonexistent_is_noop(self) -> None:
        self._db.remove("MISSING", "z", "x.jpg")
        self.assertEqual(self._db.count(), 0)

    def test_nullable_fields(self) -> None:
        self._db.upsert("ABC", "z", "a.jpg", 100)
        row = self._db.lookup("ABC", "z", "a.jpg")
        assert row is not None
        self.assertIsNone(row.version_checksum)
        self.assertIsNone(row.change_tag)

    def test_context_manager(self) -> None:
        tmpdir2 = tempfile.mkdtemp()
        try:
            with ManifestDB(tmpdir2) as db:
                db.upsert("X", "z", "x.jpg", 1)
                self.assertEqual(db.count(), 1)
            # DB should be closed but file persists
            self.assertTrue(os.path.isfile(os.path.join(tmpdir2, ".icloudpd.db")))
        finally:
            shutil.rmtree(tmpdir2)

    def test_persistence_across_opens(self) -> None:
        """Data persists after close and reopen."""
        self._db.upsert("PERSIST", "z", "p.jpg", 42)
        self._db.close()
        self._db.open()
        row = self._db.lookup("PERSIST", "z", "p.jpg")
        assert row is not None
        self.assertEqual(row.version_size, 42)
