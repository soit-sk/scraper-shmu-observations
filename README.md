# SHMU Environment Observation Data

SHMU made available for [DanubeHack 3.0](http://danubehack.eu/) some
environment obeservation data at:

http://meteo.shmu.sk/customer/home/opendata/

We can say it is a REST API with CSV payload. Thus this scraper is not "the
usual one" (thanks to REST and CSV, its job would be much easier compared to
[other scrapers](https://morph.io/soit-sk/)):

1. crawl all data
2. (optionaly) keep local copy of CSV snapshots
3. cmopile all snapshots into one SQLite database

## Data

TODO: link to SHMU data documentation

## Status

work-in-progress

## How to run

`python3 -m venv .venv`

`source .venv/bin/activate`

`pip install -U pip` # optional step

`pip install -r requirements.txt`

`python scraper.py`

# License

Scraper is licensed under BSD license, see [full text of license](LICENSE).

# Scraper is on Morph

This is a scraper that runs on [Morph](https://morph.io).  To get started
[see the documentation](https://morph.io/documentation)

# TODO

* step 2 - works, but make it configurable
* step 3
* ...
