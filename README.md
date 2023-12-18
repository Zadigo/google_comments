# Comments scraper

## Calling the spider

You can use two different types of endpoints to call the spider: `place` and `places`.

The first endpoint will help parse a single Google place url (e.g. [https://www.google.com/maps/place/Chocolat+Chapon/@48.8973502,2.0882916,17z/data=!4m15!1m8!3m7!1s0x47e6882de959a627:0x52a8c365babfeeed!2s18+Rue+de+Poissy,+78100+Saint-Germain-en-Laye!3b1!8m2!3d48.8973467!4d2.0908665!16s%2Fg%2F11c3q3x11y!3m5!1s0x47e689f6f8cf3925:0x9ab72e2475c1a6d0!8m2!3d48.8973467!4d2.0908665!16s%2Fg%2F11vjmvncpn?entry=ttu](18 Rue de Poissy, Maison Chapon)) whereas a places will parse comments for a page with multiple businesses (e.g. [https://www.google.com/maps/search/maison+chapon/@48.8252496,2.2837276,11.92z?entry=ttu](maison chapon)).

To call the spider do the following:

```python
import subprocess

args = ['python', '-m', 'maps', 'places', 'https://.../google.com/maps/search/...']
subprocess.call(args)
```

or in a terminal:

```bash
python -m maps places https://.../google.com/maps/search/...
```
