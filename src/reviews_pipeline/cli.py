from __future__ import annotations

import argparse
import json
import sys

from . import jobs
from .config import Settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='reviews-pipeline')
    sub = parser.add_subparsers(dest='cmd', required=True)
    b = sub.add_parser('bronze', help='CSV -> bronze parquet (local mode)')
    b.add_argument('--csv', required=True)
    b.add_argument('--batch-id', required=True)
    s = sub.add_parser('shard', help='bronze -> silver + gold partials for one shard')
    s.add_argument('--batch-id', required=True)
    s.add_argument('--shard', type=int, required=True)
    s.add_argument('--shard-count', type=int, required=True)
    r = sub.add_parser('reduce', help='merge gold partials into final gold tables')
    r.add_argument('--batch-id', required=True)
    a = sub.add_parser('all', help='run every stage locally')
    a.add_argument('--csv', required=True)
    a.add_argument('--batch-id', required=True)
    a.add_argument('--shard-count', type=int, default=4)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    if args.cmd == 'bronze':
        result = jobs.job_bronze(settings, args.csv, args.batch_id)
    elif args.cmd == 'shard':
        result = jobs.job_shard(settings, args.batch_id, args.shard, args.shard_count)
    elif args.cmd == 'reduce':
        result = jobs.job_reduce(settings, args.batch_id)
    else:
        result = jobs.job_all(settings, args.csv, args.batch_id, args.shard_count)
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main())