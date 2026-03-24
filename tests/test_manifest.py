"""Unit tests for icloudpd.manifest — SQLite asset manifest."""

import json
import os
import shutil
import sqlite3
import tempfile
from unittest import TestCase

from icloudpd.manifest import SCHEMA_VERSION, ManifestDB, ManifestRow


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

    def test_schema_version_set(self) -> None:
        conn = sqlite3.connect(os.path.join(self._tmpdir, ".icloudpd.db"))
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        conn.close()
        self.assertEqual(version, SCHEMA_VERSION)

    def test_empty_db_has_zero_count(self) -> None:
        self.assertEqual(self._db.count(), 0)

    def test_upsert_and_lookup_all_fields(self) -> None:
        self._db.upsert(
            asset_id="ABC123",
            zone_id="PrimarySync",
            local_path="2024-01/IMG_0001.JPG",
            version_size=1884695,
            version_checksum="chk123",
            change_tag="49lb",
            item_type="public.jpeg",
            filename="IMG_0001.JPG",
            asset_date="2024-01-15T10:30:00+11:00",
            added_date="2024-01-15T12:00:00+11:00",
            is_favorite=1,
            is_hidden=0,
            is_deleted=0,
            original_width=4032,
            original_height=3024,
            duration=None,
            orientation=6,
            title="Beach sunset",
            description="A lovely sunset at the beach",
            keywords=json.dumps(["sunset", "beach", "travel"]),
            gps_latitude=-33.795295,
            gps_longitude=151.26715,
            gps_altitude=46.3,
        )
        row = self._db.lookup("ABC123", "PrimarySync", "2024-01/IMG_0001.JPG")
        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(row.asset_id, "ABC123")
        self.assertEqual(row.version_size, 1884695)
        self.assertEqual(row.item_type, "public.jpeg")
        self.assertEqual(row.filename, "IMG_0001.JPG")
        self.assertEqual(row.is_favorite, 1)
        self.assertEqual(row.original_width, 4032)
        self.assertEqual(row.original_height, 3024)
        self.assertEqual(row.orientation, 6)
        self.assertEqual(row.title, "Beach sunset")
        self.assertEqual(row.description, "A lovely sunset at the beach")
        self.assertEqual(json.loads(row.keywords), ["sunset", "beach", "travel"])
        self.assertAlmostEqual(row.gps_latitude, -33.795295, places=5)
        self.assertAlmostEqual(row.gps_longitude, 151.26715, places=4)
        self.assertAlmostEqual(row.gps_altitude, 46.3, places=1)
        self.assertIsNotNone(row.downloaded_at)
        self.assertIsNotNone(row.last_updated_at)

    def test_lookup_missing_returns_none(self) -> None:
        self.assertIsNone(self._db.lookup("MISSING", "z", "x.jpg"))

    def test_upsert_updates_existing_row(self) -> None:
        self._db.upsert("ABC", "z", "a.jpg", 100, title="old")
        self._db.upsert("ABC", "z", "a.jpg", 200, title="new")
        self.assertEqual(self._db.count(), 1)
        row = self._db.lookup("ABC", "z", "a.jpg")
        assert row is not None
        self.assertEqual(row.version_size, 200)
        self.assertEqual(row.title, "new")

    def test_last_updated_at_changes_on_update(self) -> None:
        self._db.upsert("ABC", "z", "a.jpg", 100)
        row1 = self._db.lookup("ABC", "z", "a.jpg")
        assert row1 is not None
        import time
        time.sleep(0.01)
        self._db.upsert("ABC", "z", "a.jpg", 100, title="updated")
        row2 = self._db.lookup("ABC", "z", "a.jpg")
        assert row2 is not None
        self.assertEqual(row1.downloaded_at, row1.last_updated_at)
        # last_updated_at should change, downloaded_at should not
        self.assertNotEqual(row1.last_updated_at, row2.last_updated_at)

    def test_same_asset_different_paths(self) -> None:
        """Live photo: one asset produces JPEG + MOV."""
        self._db.upsert("LIVE1", "z", "2024-01/IMG_0001.JPG", 1000)
        self._db.upsert("LIVE1", "z", "2024-01/IMG_0001.MOV", 5000)
        self.assertEqual(self._db.count(), 2)

    def test_same_asset_different_zones(self) -> None:
        self._db.upsert("DUP1", "PrimarySync", "a.jpg", 100)
        self._db.upsert("DUP1", "SharedSync-XYZ", "a.jpg", 100)
        self.assertEqual(self._db.count(), 2)

    def test_lookup_by_path(self) -> None:
        self._db.upsert("ABC", "z", "2024-01/IMG.JPG", 1000)
        row = self._db.lookup_by_path("2024-01/IMG.JPG")
        assert row is not None
        self.assertEqual(row.asset_id, "ABC")

    def test_remove(self) -> None:
        self._db.upsert("ABC", "z", "a.jpg", 100)
        self._db.remove("ABC", "z", "a.jpg")
        self.assertEqual(self._db.count(), 0)

    def test_remove_by_path(self) -> None:
        self._db.upsert("ABC", "z", "a.jpg", 100)
        self._db.upsert("DEF", "z", "a.jpg", 200)
        self._db.remove_by_path("a.jpg")
        self.assertEqual(self._db.count(), 0)

    def test_context_manager(self) -> None:
        tmpdir2 = tempfile.mkdtemp()
        try:
            with ManifestDB(tmpdir2) as db:
                db.upsert("X", "z", "x.jpg", 1)
                self.assertEqual(db.count(), 1)
            self.assertTrue(os.path.isfile(os.path.join(tmpdir2, ".icloudpd.db")))
        finally:
            shutil.rmtree(tmpdir2)

    def test_persistence_across_opens(self) -> None:
        self._db.upsert("PERSIST", "z", "p.jpg", 42, title="hello")
        self._db.close()
        self._db.open()
        row = self._db.lookup("PERSIST", "z", "p.jpg")
        assert row is not None
        self.assertEqual(row.version_size, 42)
        self.assertEqual(row.title, "hello")

    def test_nullable_metadata_fields(self) -> None:
        self._db.upsert("ABC", "z", "a.jpg", 100)
        row = self._db.lookup("ABC", "z", "a.jpg")
        assert row is not None
        self.assertIsNone(row.version_checksum)
        self.assertIsNone(row.title)
        self.assertIsNone(row.gps_latitude)
        self.assertIsNone(row.duration)
        self.assertEqual(row.is_favorite, 0)

    def test_keywords_stored_as_json(self) -> None:
        kw = ["sunset", "beach"]
        self._db.upsert("ABC", "z", "a.jpg", 100, keywords=json.dumps(kw))
        row = self._db.lookup("ABC", "z", "a.jpg")
        assert row is not None
        self.assertEqual(json.loads(row.keywords), kw)


class TestManifestMigration(TestCase):
    def test_migrate_from_v0_schema(self) -> None:
        """A pre-versioned DB (7 columns) should be migrated to the full schema."""
        tmpdir = tempfile.mkdtemp()
        try:
            db_path = os.path.join(tmpdir, ".icloudpd.db")
            # Create a v0 DB with only the original 7 columns
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
                ("OLD1", "z", "old.jpg", 999, None, "t1", "2026-01-01T00:00:00+00:00"),
            )
            conn.commit()
            conn.close()

            # Open with ManifestDB — should migrate
            db = ManifestDB(tmpdir)
            db.open()

            # Verify version was set
            version = db._db.execute("PRAGMA user_version").fetchone()[0]
            self.assertEqual(version, SCHEMA_VERSION)

            # Verify index was created during migration
            indexes = db._db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='manifest'"
            ).fetchall()
            index_names = [row[0] for row in indexes]
            self.assertIn("idx_manifest_path", index_names)

            # Verify old row survived with new columns as defaults
            row = db.lookup("OLD1", "z", "old.jpg")
            assert row is not None
            self.assertEqual(row.asset_id, "OLD1")
            self.assertEqual(row.version_size, 999)
            self.assertEqual(row.last_updated_at, "")  # default from migration
            self.assertIsNone(row.title)
            self.assertEqual(row.is_favorite, 0)

            # Verify new columns are writable
            db.upsert("NEW1", "z", "new.jpg", 500, title="test", is_favorite=1)
            row2 = db.lookup("NEW1", "z", "new.jpg")
            assert row2 is not None
            self.assertEqual(row2.title, "test")
            self.assertEqual(row2.is_favorite, 1)

            db.close()
        finally:
            shutil.rmtree(tmpdir)
