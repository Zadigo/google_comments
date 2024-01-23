# Comments scraper

## Calling the spider

You can use four different types of endpoints to call one of the following spiders :
* GooglePlaces
* GooglePlace
* SearchLinks
* SearchBusinesses

## Google places

This first enpoint will parse comments for a page with multiple businesses (e.g. [Maison Chapon](https://www.google.com/maps/search/maison+chapon/@48.8252496,2.2837276,11.92z?entry=ttu)).


## Google place

This endpoint will help parse a single Google place url (e.g. [18 Rue de Poissy, Maison Chapon](https://www.google.com/maps/place/Chocolat+Chapon/@48.8973502,2.0882916,17z/data=!4m15!1m8!3m7!1s0x47e6882de959a627:0x52a8c365babfeeed!2s18+Rue+de+Poissy,+78100+Saint-Germain-en-Laye!3b1!8m2!3d48.8973467!4d2.0908665!16s%2Fg%2F11c3q3x11y!3m5!1s0x47e689f6f8cf3925:0x9ab72e2475c1a6d0!8m2!3d48.8973467!4d2.0908665!16s%2Fg%2F11vjmvncpn?entry=ttu)) 

You can either call the spider for a single url for a list of urls that you would like to have the comments for. In the latter case, you file needs to be called `google_place_urls.csv` and be placed in the `/media/` folder of the app. The following example shows how to run this specific spider inline:

```python
instance = GooglePlace(output_folder='my_folder_name')
instance.iterate_urls()
```


## Calling a spider

To call `place` or `places`, do the following:

```python
import subprocess

args = ['python', '-m', 'maps', 'places', 'https://.../google.com/maps/search/...']
subprocess.call(args)
```

or in a terminal:

```bash
python -m maps places https://.../google.com/maps/search/...
```
