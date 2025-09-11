from __future__ import annotations

from typing import Union
import numpy as np
from numpy.typing import ArrayLike, NDArray

EARTH_RADIUS_KM: float = 6371.0088


def haversine_km(
    lat1: ArrayLike,
    lon1: ArrayLike,
    lat2: ArrayLike,
    lon2: ArrayLike,
) -> Union[NDArray[np.float64], float]:
    """
    Compute great-circle distances using the haversine formula.

    Parameters
    ----------
    lat1 : ArrayLike
        Latitude(s) of the first point in degrees.
    lon1 : ArrayLike
        Longitude(s) of the first point in degrees.
    lat2 : ArrayLike
        Latitude(s) of the second point in degrees.
    lon2 : ArrayLike
        Longitude(s) of the second point in degrees.

    Returns
    -------
    numpy.ndarray or float
        Distance(s) in kilometres. Shapes follow NumPy broadcasting rules.

    Notes
    -----
    - Vectorized: scalars, lists, and ndarrays are supported and broadcast.
    - Assumes a spherical Earth with mean radius ``EARTH_RADIUS_KM = 6371.0088``.
    - ``NaN`` in any input propagates to the corresponding output.
    - ``np.clip`` is used to keep the argument of ``arcsin`` in ``[0, 1]`` to
      avoid numerical issues on edge cases.

    Examples
    --------
    >>> round(float(haversine_km(0, 0, 0, 1)), 3)  # ~ 1° lon at equator
    111.195
    """
    lat1_rad = np.radians(np.asarray(lat1, dtype=np.float64))
    lon1_rad = np.radians(np.asarray(lon1, dtype=np.float64))
    lat2_rad = np.radians(np.asarray(lat2, dtype=np.float64))
    lon2_rad = np.radians(np.asarray(lon2, dtype=np.float64))

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
    a = np.clip(a, 0.0, 1.0)  # numerical safety
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))
