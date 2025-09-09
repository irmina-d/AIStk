
import numpy as np

EARTH_RADIUS_KM = 6371.0088

def haversine_km(lat1, lon1, lat2, lon2):
    """Vectorized haversine distance (km). Inputs are in degrees (numpy arrays or scalars)."""
    rlat1 = np.radians(lat1); rlon1 = np.radians(lon1)
    rlat2 = np.radians(lat2); rlon2 = np.radians(lon2)
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = np.sin(dlat/2.0)**2 + np.cos(rlat1)*np.cos(rlat2)*np.sin(dlon/2.0)**2
    return 2.0 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a))
