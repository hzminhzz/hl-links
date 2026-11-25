"""Run-bound immutable freeze and verification receipt identities."""
import json
from pathlib import Path

from .study import PILOT_CUTOFF as T, _code_digest, _sha256, _utc

# The original executed pilot predates explicit run and semantic identities.
_REVIEWED_CODE = "ed3565187e24756b6d9834b4ef66c3c9153f2e2f9fbb4518944385ee16a7d5c8"
_REVIEWED_SEMANTICS = {
    "fills.py": "17d0efbf5587b706f6e7a8fb073881519340d8fcde5881c72db098e6fec15861",
    "ledger.py": "bebbbfea1402737e5d575012b937187cc3a96f2728da48381623690c3cfe10c0",
    "cohorts.py": "50d4caf2075decf1027cdc06addf7ae71a089003790559e7fc879346db91fb8e",
    "study.py": "058ed9e1ced305b37b59eaff0a0e344cdffb838e5a725f6b7d8f32e22efed95d",
    "acquire.py": "1349d89ea7fa9eece5b153d250b7d70bdac5aeae138336690530174b9383c782",
}


def research_semantics():
    return {name: _sha256(Path(__file__).with_name(name)) for name in _REVIEWED_SEMANTICS}


def run_identity(root, analysis):
    return {"root": str(root.resolve()), "analysis": str(analysis.resolve())}


def load_checkpoint(directory):
    path = directory / "manifest.json"
    receipt = json.loads((directory / "receipt.json").read_text())
    if _sha256(path) != receipt["sha256"]:
        raise ValueError("Checkpoint checksum mismatch")
    return json.loads(path.read_text())


def validate_frozen(root, analysis, frozen=None, *, code_sha256=None):
    """Bind a durable freeze to its own acquisition and analysis directories."""
    root, analysis = Path(root).resolve(), Path(analysis).resolve()
    saved = load_checkpoint(analysis / "frozen")
    if (frozen is not None and frozen != saved) or _utc(saved["cutoff"]) != T:
        raise ValueError("Frozen historical checkpoint changed")
    identity = saved.get("run_identity")
    days = saved["history_receipt"]["days"]
    if identity is not None:
        if identity != run_identity(root, analysis):
            raise ValueError("Frozen run identity mismatch")
    elif not days:
        raise ValueError("Legacy freeze lacks historical run identity")
    # Legacy receipts use paths relative to the original checkout. New ones
    # also have an explicit root binding, including an empty historical run.
    for item in days:
        path = Path(item["path"]).resolve()
        label = item["day"]
        original = root / label / "fills.parquet"
        replacements = root / ".replacements" / label
        if path != original and not (
            path.is_relative_to(replacements)
            and path.parent.name == label and path.name == "fills.parquet"
        ):
            raise ValueError("Frozen historical run identity mismatch")
    for item in saved["history_partitions"]:
        if Path(item["path"]).resolve().parent != analysis / "history-parts":
            raise ValueError("Frozen analysis run identity mismatch")
    current = code_sha256 if code_sha256 is not None else _code_digest()
    if saved["code_sha256"] != current:
        expected = saved.get("research_semantics")
        if expected is None and saved["code_sha256"] == _REVIEWED_CODE:
            expected = _REVIEWED_SEMANTICS
        if expected != research_semantics():
            raise ValueError("Frozen pilot code changed; do not silently refreeze")
    return saved


def verification_directory(analysis, code_sha256):
    """Keep the first replay receipt; give compatible later code its own receipt."""
    base = analysis / "verified-v2"
    if base.exists():
        prior = load_checkpoint(base)
        if prior["verification_code_sha256"] != code_sha256:
            return analysis / f"verified-v2-{code_sha256}"
    return base
