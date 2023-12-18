import subprocess
import unittest

TEST_URL = 'https://www.google.com/maps/place/CHAPON+CHOCOLATERIE/@48.881444,2.2358281,17z/data=!3m1!4b1!4m6!3m5!1s0x47e6650186a902d5:0x17a3c197aff7990b!8m2!3d48.8814405!4d2.238403!16s%2Fg%2F11stsq4rdf?entry=ttu'


class TestSpider(unittest.TestCase):
    def test_start_spider(self):
        args = ['python', '-m', 'maps', 'place', TEST_URL]
        subprocess.call(args)


if __name__ == '__main__':
    unittest.main()
