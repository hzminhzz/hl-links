"""Local research resource limits; never spill data into an unignored directory."""

from contextlib import contextmanager
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import duckdb

GIB = 1024**3
MEMORY_LIMIT = "4GiB"
SPILL_LIMIT = "12GiB"
SPILL_LIMIT_BYTES = 12 * GIB
MIN_FREE_DISK_BYTES = 16 * GIB
DEFAULT_MAX_SNAPSHOT_BYTES = 32 * GIB
THREADS = 4


class SnapshotBudgetExceeded(RuntimeError):
    """Stop an acquisition without publishing a successful manifest."""


def private_runs_dir():
    root = Path(__file__).resolve().parents[2] / "runs"
    root.mkdir(mode=0o700, exist_ok=True)
    if root.resolve() != root:
        raise ValueError("The private runs directory cannot be a symlink")
    return root


def configure_duckdb(connection, spill_dir):
    spill_dir = Path(spill_dir).resolve()
    spill_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    connection.execute("SET temp_directory = ?", [str(spill_dir)])
    connection.execute("SET memory_limit = ?", [MEMORY_LIMIT])
    connection.execute("SET max_temp_directory_size = ?", [SPILL_LIMIT])
    connection.execute("SET threads = ?", [THREADS])
    connection.execute("SET TimeZone = 'UTC'")


def require_disk_headroom(path, additional_bytes=0):
    if shutil.disk_usage(path).free < MIN_FREE_DISK_BYTES + additional_bytes:
        raise SnapshotBudgetExceeded("private snapshot disk headroom exhausted")


@contextmanager
def bounded_duckdb():
    """Read-only analytical queries with private spill and the standard budget."""
    root = private_runs_dir()
    require_disk_headroom(root, SPILL_LIMIT_BYTES)
    with TemporaryDirectory(prefix=".query-", dir=root) as spill:
        with duckdb.connect() as con:
            configure_duckdb(con, spill)
            yield con


def duckdb_budget():
    return {"memory_limit": MEMORY_LIMIT, "spill_limit": SPILL_LIMIT,
            "threads": THREADS, "minimum_free_disk_bytes": MIN_FREE_DISK_BYTES}
