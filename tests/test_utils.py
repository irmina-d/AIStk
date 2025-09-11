import numpy as np
from aistk.utils import haversine_km


def test_haversine_zero():
    assert haversine_km(0.0, 0.0, 0.0, 0.0) == 0.0


def test_haversine_equator_one_degree_lon():
    # ~111.319 km na równiku na 1 stopień długości
    d = haversine_km(0.0, 0.0, 0.0, 1.0)
    assert np.isclose(d, 111.319, atol=0.5)
