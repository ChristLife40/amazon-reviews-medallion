"""Move data between ADLS Gen2 and the local work directory (used when STORAGE_MODE=azure)."""
from __future__ import annotations

import shutil

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from .config import Settings


def _fs(settings: Settings, layer: str):
    credential = DefaultAzureCredential(
        managed_identity_client_id=settings.managed_identity_client_id or None
    )
    service = DataLakeServiceClient(
        f'https://{settings.storage_account}.dfs.core.windows.net', credential=credential
    )
    return service.get_file_system_client(layer)


def sync_down(settings: Settings, layer: str, prefix: str) -> None:
    fs = _fs(settings, layer)
    local_root = settings.path(layer, prefix)
    if local_root.exists():
        shutil.rmtree(local_root)
    for item in fs.get_paths(path=prefix, recursive=True):
        if item.is_directory:
            continue
        target = settings.path(layer, item.name)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, 'wb') as handle:
            fs.get_file_client(item.name).download_file().readinto(handle)


def sync_up(settings: Settings, layer: str, prefix: str) -> None:
    fs = _fs(settings, layer)
    local = settings.path(layer, prefix)
    if not local.exists():
        raise FileNotFoundError(local)
    if local.is_file():
        files = [local]
    else:
        files = [f for f in sorted(local.rglob('*')) if f.is_file()]
        try:  # remove stale files from an earlier run so a rerun never duplicates data
            fs.get_directory_client(prefix).delete_directory()
        except ResourceNotFoundError:
            pass
    base = settings.path(layer)
    for f in files:
        if f.name.startswith('.'):  # skip Spark checksum files (.crc)
            continue
        with open(f, 'rb') as handle:
            fs.get_file_client(f.relative_to(base).as_posix()).upload_data(handle, overwrite=True)