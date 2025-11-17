import sys
from pathlib import Path
import unittest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils_geo import haversine_km


class TestUtilsGeo(unittest.TestCase):
    def test_haversine_zero(self):
        d = haversine_km(34.0, -6.0, 34.0, -6.0)
        self.assertAlmostEqual(d, 0.0, places=3)

    def test_haversine_simple(self):
        rabat = (34.020882, -6.841650)
        casa = (33.573110, -7.589843)
        d = haversine_km(rabat[0], rabat[1], casa[0], casa[1])
        self.assertTrue(70 <= d <= 110)


if __name__ == "__main__":
    unittest.main()
