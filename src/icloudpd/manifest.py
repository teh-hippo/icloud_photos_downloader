"""SQLite asset manifest for identity-based sync tracking.

Replaces fragile size-based dedup with definitive identity matching using
iCloud's recordName (asset_id). Stores the iCloud-reported version_size
(pre-EXIF-injection) so size comparisons are always against the original,
not the locally-modified file.

The manifest DB lives at {download_dir}/.icloudpd.db and travels with
the photo library if the directory is moved.
"""

import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS manifest (
    asset_id TEXT NOT NULL,
    zone_id TEXT NOT NULL DEFAULT '',
    local_path TEXT NOT NULL,
    version_size INTEGER NOT NULL,
    version_checksum TEXT,
    change_tag TEXT,
    downloaded_at TEXT NOT NULL,
    PRIMARY KEY (asset_id, zone_id, local_path)
);
CREATE INDEX IF NOT EXISTS idx_manifest_path ON manifest(local_path);
"""


@dataclass(frozen=True)
class ManifestRow:
    """A single manifest entry."""

    asset_id: str
    zone_id: str
    local_path: str
    version_size: int
    version_checksum: str | None
    change_tag: str | None
    downloaded_at: str


class ManifestDB:
    """SQLite-backed asset manifest for tracking downloaded files."""

    def __init__(self, download_dir: str) -> None:
        self._db_path = os.path.join(download_dir, ".icloudpd.db")
        self._conn: sqlite3.Connection | None = None
        self._dirty = False
        self.zone_id: str = ""  # Set per-library before sync loop

    @property
    def _db(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("ManifestDB is not open")
        return self._conn

    def open(self) -> None:
        """Open the manifest DB, creating it and the schema if needed."""
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._dirty = False

    def close(self) -> None:
        """Close the manifest DB, committing any pending writes."""
        if self._conn:
            if self._dirty:
                self._conn.commit()
                self._dirty = False
            self._conn.close()
            self._conn = None

    def flush(self) -> None:
        """Commit pending writes without closing."""
        if self._conn and self._dirty:
            self._conn.commit()
            self._dirty = False

    def __enter__(self) -> "ManifestDB":
        self.open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def lookup(self, asset_id: str, zone_id: str, local_path: str) -> ManifestRow | None:
        """Look up a manifest entry by identity."""
        row = self._db.execute(
            "SELECT asset_id, zone_id, local_path, version_size, version_checksum, "
            "change_tag, downloaded_at FROM manifest "
            "WHERE asset_id = ? AND zone_id = ? AND local_path = ?",
            (asset_id, zone_id, local_path),
        ).fetchone()
        if row is None:
            return None
        return ManifestRow(*row)

    def lookup_by_path(self, local_path: str) -> ManifestRow | None:
        """Look up a manifest entry by local path (for backward compat adoption)."""
        row = self._db.execute(
            "SELECT asset_id, zone_id, local_path, version_size, version_checksum, "
            "change_tag, downloaded_at FROM manifest "
            "WHERE local_path = ? LIMIT 1",
            (local_path,),
        ).fetchone()
        if row is None:
            return None
        return ManifestRow(*row)

    def upsert(
        self,
        asset_id: str,
        zone_id: str,
        local_path: str,
        version_size: int,
        version_checksum: str | None = None,
        change_tag: str | None = None,
    ) -> None:
        """Insert or update a manifest entry. Batched — call flush() to persist."""
        now = datetime.now(tz=timezone.utc).isoformat()
        self._db.execute(
            "INSERT INTO manifest (asset_id, zone_id, local_path, version_size, "
            "version_checksum, change_tag, downloaded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(asset_id, zone_id, local_path) DO UPDATE SET "
            "version_size=excluded.version_size, "
            "version_checksum=excluded.version_checksum, "
            "change_tag=excluded.change_tag, "
            "downloaded_at=excluded.downloaded_at",
            (asset_id, zone_id, local_path, version_size, version_checksum, change_tag, now),
        )
        self._dirty = True

    def remove(self, asset_id: str, zone_id: str, local_path: str) -> None:
        """Remove a manifest entry."""
        self._db.execute(
            "DELETE FROM manifest WHERE asset_id = ? AND zone_id = ? AND local_path = ?",
            (asset_id, zone_id, local_path),
        )
        self._dirty = True

    def remove_by_path(self, local_path: str) -> None:
        """Remove all manifest entries for a local path (used by autodelete)."""
        self._db.execute(
            "DELETE FROM manifest WHERE local_path = ?",
            (local_path,),
        )
        self._dirty = True

    def count(self) -> int:
        """Return the total number of manifest entries."""
        row = self._db.execute("SELECT COUNT(*) FROM manifest").fetchone()
        return row[0] if row else 0
