"""ZIP ingestion endpoints for the EDA Dashboard API."""

from __future__ import annotations

import os
import shutil
import tempfile
import time
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from threading import Lock
from typing import Counter, Dict, List

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel, Field

from storage.duck import ingest_combined_files

router = APIRouter()

SUPPORTED_SUFFIXES = (".csv", ".csv.gz", ".parquet")
UPLOAD_CHUNK_SIZE = 1024 * 1024
SESSION_TTL_SECONDS = 60 * 30

_zip_sessions: Dict[str, Dict[str, object]] = {}
_session_lock = Lock()


class ZipIngestRequest(BaseModel):
    zip_id: str = Field(..., description="UUID returned by /upload_zip")
    selected_files: List[str] = Field(..., min_length=1)
    dataset_name: str | None = None


def _is_supported(filename: str) -> bool:
    lower = filename.lower()
    return any(lower.endswith(suffix) for suffix in SUPPORTED_SUFFIXES)


def _sanitize_dataset_name(name: str | None) -> str:
    if not name:
        return ""
    sanitized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    return sanitized.strip("_")


async def _write_upload_to_disk(upload: UploadFile, path: Path) -> int:
    written = 0
    with open(path, "wb") as buffer:
        while True:
            chunk = await upload.read(UPLOAD_CHUNK_SIZE)
            if not chunk:
                break
            buffer.write(chunk)
            written += len(chunk)
    return written


def _extract_zip(archive: Path, extract_dir: Path) -> Dict[str, str]:
    files: Dict[str, str] = {}
    base = extract_dir.resolve()

    with zipfile.ZipFile(archive) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue

            relative = PurePosixPath(member.filename)
            if relative.is_absolute() or ".." in relative.parts:
                raise HTTPException(
                    status_code=400, detail="ZIP file contains unsafe file paths"
                )

            dest_path = (extract_dir / Path(relative)).resolve()
            if not str(dest_path).startswith(str(base)):
                raise HTTPException(
                    status_code=400, detail="ZIP file contains unsafe file paths"
                )

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as src, open(dest_path, "wb") as dst:
                shutil.copyfileobj(src, dst, UPLOAD_CHUNK_SIZE)

            files[relative.as_posix()] = str(dest_path)

    return files


def _prune_sessions() -> None:
    cutoff = time.time() - SESSION_TTL_SECONDS
    expired: List[str] = []
    for key, session in list(_zip_sessions.items()):
        created = session.get("created_at")
        if isinstance(created, (int, float)) and created < cutoff:
            expired.append(key)

    for key in expired:
        session = _zip_sessions.pop(key, None)
        if not session:
            continue
        base_dir = Path(session.get("base_dir", ""))
        if base_dir.exists():
            shutil.rmtree(base_dir, ignore_errors=True)


def _store_session(zip_id: str, payload: Dict[str, object]) -> None:
    with _session_lock:
        _prune_sessions()
        _zip_sessions[zip_id] = payload


def _get_session(zip_id: str) -> Dict[str, object]:
    with _session_lock:
        session = _zip_sessions.get(zip_id)
    if not session:
        raise HTTPException(status_code=404, detail="ZIP session not found")
    return session


def _cleanup_session(zip_id: str) -> None:
    with _session_lock:
        session = _zip_sessions.pop(zip_id, None)
    if not session:
        return
    base_dir = Path(session.get("base_dir", ""))
    if base_dir.exists():
        shutil.rmtree(base_dir, ignore_errors=True)


@router.post("/upload_zip")
async def upload_zip(file: UploadFile = File(...)):
    if not file.filename or not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Only ZIP files are supported")

    base_dir = Path(tempfile.mkdtemp(prefix="zip_upload_"))
    archive_path = base_dir / "upload.zip"
    extract_dir = base_dir / "contents"
    extract_dir.mkdir(parents=True, exist_ok=True)
    original_name = file.filename

    try:
        bytes_written = await _write_upload_to_disk(file, archive_path)
        await file.close()
    except Exception:
        shutil.rmtree(base_dir, ignore_errors=True)
        raise

    if bytes_written == 0:
        shutil.rmtree(base_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail="Uploaded ZIP file is empty")

    try:
        files_map = _extract_zip(archive_path, extract_dir)
    except HTTPException:
        shutil.rmtree(base_dir, ignore_errors=True)
        raise
    except zipfile.BadZipFile:
        shutil.rmtree(base_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail="Invalid ZIP file")
    finally:
        archive_path.unlink(missing_ok=True)

    valid_files = [name for name in files_map if _is_supported(name)]

    if not valid_files:
        shutil.rmtree(base_dir, ignore_errors=True)
        raise HTTPException(
            status_code=400,
            detail="ZIP archive must include CSV, CSV.GZ, or Parquet files",
        )

    # Count invalid files by suffix
    invalid_files = [name for name in files_map if not _is_supported(name)]
    invalid_suffix_counts = {}
    for filename in invalid_files:
        # Get the file extension (e.g., ".txt", ".jpg")
        suffix = Path(filename).suffix.lower() or "(no extension)"
        invalid_suffix_counts[suffix] = invalid_suffix_counts.get(suffix, 0) + 1

    zip_id = str(uuid.uuid4())
    dataset_hint = (
        _sanitize_dataset_name(Path(original_name).stem) or f"zip_{zip_id[:8]}"
    )

    _store_session(
        zip_id,
        {
            "base_dir": str(base_dir),
            "files": files_map,
            "source_name": original_name,
            "suggested_dataset": dataset_hint,
            "created_at": time.time(),
        },
    )

    return {
        "zip_id": zip_id,
        "files": sorted(valid_files),
        "invalid_suffix_counts": invalid_suffix_counts,
    }


@router.post("/ingest_zip_contents")
def ingest_zip_contents(request: ZipIngestRequest):
    session = _get_session(request.zip_id)
    selected = request.selected_files

    if not selected:
        raise HTTPException(
            status_code=400, detail="At least one file must be selected"
        )

    available_files: Dict[str, str] = session["files"]
    missing = [name for name in selected if name not in available_files]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Files not found in ZIP: {', '.join(missing)}",
        )

    invalid_counts = Counter(
        os.path.splitext(name)[1].lower()
        for name in selected
        if not _is_supported(name)
    )
    invalid_counts.pop("", None)

    if invalid_counts:
        raise HTTPException(
            status_code=400,
            detail=(
                "Unsupported file types selected: "
                + ", ".join(f"{ext} ({count})" for ext, count in invalid_counts.items())
            ),
        )

    file_paths = []
    empty_files = []
    for name in selected:
        path = Path(available_files[name])
        if not path.exists():
            raise HTTPException(
                status_code=400, detail=f"File missing on server: {name}"
            )
        if path.stat().st_size == 0:
            empty_files.append(name)
        file_paths.append(str(path))

    if empty_files:
        raise HTTPException(
            status_code=400,
            detail=f"Selected files are empty: {', '.join(empty_files)}",
        )

    dataset_label = request.dataset_name or session["suggested_dataset"]
    dataset_id = _sanitize_dataset_name(dataset_label)
    if not dataset_id:
        dataset_id = f"zip_{request.zip_id[:8]}"

    source_label = f"zip:{session['source_name']}::{','.join(selected)}"

    try:
        table_name, n_rows, n_cols = ingest_combined_files(
            file_paths, dataset_id, source_label
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=500, detail=f"Failed to ingest files: {exc}"
        ) from exc
    finally:
        _cleanup_session(request.zip_id)

    return {
        "status": "success",
        "rows_loaded": n_rows,
        "dataset_id": dataset_id,
        "table_name": table_name,
        "columns": n_cols,
        "files_ingested": selected,
    }
