"""Top-level package for the AIS Toolkit."""

from importlib import metadata

try:  # pragma: no cover - best effort when package metadata is available
    __version__ = metadata.version("aistk")
except metadata.PackageNotFoundError:  # pragma: no cover - during local development
    __version__ = "0.1.0"

from .core import AISDataset  # noqa: E402
from .events import detect_events_df  # noqa: E402
from .stats import compute_stats_df  # noqa: E402
from .viz import plot_track_html  # noqa: E402

__all__: list[str] = [
    "AISDataset",
    "compute_stats_df",
    "detect_events_df",
    "plot_track_html",
    "__version__",
]

del metadata
