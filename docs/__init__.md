# __init__.py — Package Export Surface

## Purpose
Defines package metadata, version, and the public API (`__all__`) exposed at import time.

## Responsibilities
- Expose top-level imports (e.g., `from aisdataset import core, stats`).
- Hold `__version__` and optional runtime checks.

## Interactions with Other Modules
- All modules (re-export)

## Usage Example
```python
import aisdataset as ais
print(ais.__version__)
```

## Public API (Outline)
**Top-level variables:** `__all__`, `__version__`

## Notes & Design Considerations
- Assumes canonical AIS columns after `schema.validate_columns()`.
- Keep I/O and analytics separated for testability.
- Prefer vectorized operations; avoid per-row Python loops where possible.