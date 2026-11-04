import json

from .config import Settings


class DataQualityError(RuntimeError):
    """Raised when a control total or quality gate fails; the pipeline must stop."""


def write_metrics(settings: Settings, batch_id: str, name: str, metrics: dict):
    path = settings.path('logs', batch_id, f'{name}.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2, default=str))
    return path