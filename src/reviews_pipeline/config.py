from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    storage_mode: str = 'local'  # local | azure
    data_root: Path = Path('data')
    storage_account: str = ''
    managed_identity_client_id: str = ''
    shuffle_partitions: int = 8
    driver_memory: str = '2g'

    @staticmethod
    def from_env() -> Settings:
        return Settings(
            storage_mode=os.getenv('STORAGE_MODE', 'local'),
            data_root=Path(os.getenv('DATA_ROOT', 'data')),
            storage_account=os.getenv('AZURE_STORAGE_ACCOUNT', ''),
            managed_identity_client_id=os.getenv('AZURE_CLIENT_ID', ''),
            shuffle_partitions=int(os.getenv('SPARK_SHUFFLE_PARTITIONS', '8')),
            driver_memory=os.getenv('SPARK_DRIVER_MEMORY', '2g'),
        )

    def path(self, layer: str, *parts: str) -> Path:
        """Local path of a lake layer: data_root/<layer>/<parts...>."""
        return self.data_root.joinpath(layer, *parts)