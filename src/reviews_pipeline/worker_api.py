from __future__ import annotations

import socket
import threading

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from . import jobs
from .config import Settings

BATCH_PATTERN = r'^[A-Za-z0-9_-]{1,64}$'

app = FastAPI(title='reviews-worker', version='0.1.0')
_busy = threading.Lock()


class ShardRequest(BaseModel):
    batch_id: str = Field(pattern=BATCH_PATTERN)
    shard: int = Field(ge=0, le=63)
    shard_count: int = Field(ge=1, le=64)


class ReduceRequest(BaseModel):
    batch_id: str = Field(pattern=BATCH_PATTERN)


@app.get('/health')
def health() -> dict:
    return {'status': 'ok', 'host': socket.gethostname(), 'busy': _busy.locked()}


def _run(fn, *args) -> dict:
    if not _busy.acquire(blocking=False):
        raise HTTPException(status_code=503, detail='worker busy, retry', headers={'Retry-After': '30'})
    try:
        return fn(Settings.from_env(), *args)
    finally:
        _busy.release()


@app.post('/process')
def process(req: ShardRequest) -> dict:
    if req.shard >= req.shard_count:
        raise HTTPException(status_code=422, detail='shard must be below shard_count')
    return _run(jobs.job_shard, req.batch_id, req.shard, req.shard_count)


@app.post('/reduce')
def reduce(req: ReduceRequest) -> dict:
    return _run(jobs.job_reduce, req.batch_id)