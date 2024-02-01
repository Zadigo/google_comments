import subprocess
import unittest

TEST_URL = 'https://www.google.com/maps/place/Chocolat+Chapon/@48.8973502,2.0882916,17z/data=!4m15!1m8!3m7!1s0x47e6882de959a627:0x52a8c365babfeeed!2s18+Rue+de+Poissy,+78100+Saint-Germain-en-Laye!3b1!8m2!3d48.8973467!4d2.0908665!16s%2Fg%2F11c3q3x11y!3m5!1s0x47e689f6f8cf3925:0x9ab72e2475c1a6d0!8m2!3d48.8973467!4d2.0908665!16s%2Fg%2F11vjmvncpn?entry=ttu'


class TestSpider(unittest.TestCase):
    def test_start_spider(self):
        args = ['python', '-m', 'maps', 'place', TEST_URL]
        subprocess.call(args)


if __name__ == '__main__':
    unittest.main()
