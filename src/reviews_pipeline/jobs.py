from __future__ import annotations

import socket
import time
from contextlib import contextmanager

from . import bronze as bronze_mod
from . import gold, silver
from .config import Settings
from .dq import write_metrics
from .spark_session import get_spark


@contextmanager
def _spark(settings: Settings, spark=None):
    if spark is not None:  # caller owns the session (tests, 'all' command)
        yield spark
        return
    session = get_spark(settings)
    try:
        yield session
    finally:
        session.stop()


def _azure(settings: Settings) -> bool:
    return settings.storage_mode == 'azure'


def _meta(stage: str, batch_id: str, started: float) -> dict:
    return {
        'stage': stage, 'batch_id': batch_id,
        'seconds': round(time.time() - started, 1), 'host': socket.gethostname(),
    }


def job_bronze(settings: Settings, csv_path, batch_id: str, spark=None) -> dict:
    """Local-only: in Azure, Data Factory's Copy activity produces bronze."""
    started = time.time()
    with _spark(settings, spark) as s:
        out = settings.path('bronze', 'reviews', batch_id)
        bronze_mod.write_bronze(bronze_mod.build_bronze(s, csv_path, batch_id), out)
        rows = s.read.parquet(str(out)).count()
    metrics = {**_meta('bronze', batch_id, started), 'rows': rows}
    write_metrics(settings, batch_id, 'bronze', metrics)
    return metrics


def job_shard(settings: Settings, batch_id: str, shard: int, shard_count: int, spark=None) -> dict:
    started = time.time()
    if _azure(settings):
        from . import io_azure
        io_azure.sync_down(settings, 'bronze', f'reviews/{batch_id}')
    with _spark(settings, spark) as s:
        metrics, clean = silver.run_silver_shard(s, settings, batch_id, shard, shard_count)
        metrics.update(gold.run_gold_partials(s, settings, batch_id, shard, clean))
        clean.unpersist()
    metrics.update({**_meta('shard', batch_id, started), 'shard': shard, 'shard_count': shard_count})
    write_metrics(settings, batch_id, f'shard_{shard}', metrics)
    if _azure(settings):
        io_azure.sync_up(settings, 'silver', f'reviews/{batch_id}/shard={shard}')
        io_azure.sync_up(settings, 'silver', f'rejects/{batch_id}/shard={shard}')
        for name in gold.PARTIAL_TABLES:
            io_azure.sync_up(settings, 'gold', f'_partials/{name}/{batch_id}/shard={shard}')
        io_azure.sync_up(settings, 'logs', f'{batch_id}/shard_{shard}.json')
    return metrics


def job_reduce(settings: Settings, batch_id: str, spark=None) -> dict:
    started = time.time()
    if _azure(settings):
        from . import io_azure
        io_azure.sync_down(settings, 'silver', f'reviews/{batch_id}')
        for name in gold.PARTIAL_TABLES:
            io_azure.sync_down(settings, 'gold', f'_partials/{name}/{batch_id}')
    with _spark(settings, spark) as s:
        metrics = gold.run_reduce(s, settings, batch_id)
    metrics.update(_meta('reduce', batch_id, started))
    write_metrics(settings, batch_id, 'reduce', metrics)
    if _azure(settings):
        for name in gold.FINAL_TABLES:
            io_azure.sync_up(settings, 'gold', name)
        io_azure.sync_up(settings, 'logs', f'{batch_id}/reduce.json')
    return metrics


def job_all(settings: Settings, csv_path, batch_id: str, shard_count: int = 4) -> dict:
    """Local convenience: bronze, every shard, then reduce, in one Spark session."""
    started = time.time()
    with _spark(settings) as s:
        result = {'bronze': job_bronze(settings, csv_path, batch_id, spark=s)}
        result['shards'] = [
            job_shard(settings, batch_id, k, shard_count, spark=s) for k in range(shard_count)
        ]
        result['reduce'] = job_reduce(settings, batch_id, spark=s)
    result['total_seconds'] = round(time.time() - started, 1)
    return result