To get started with the Normalized Tags approach.

Getting Started (normalized_tags)

Get your Python virtual environment setup.  We use pipenv, but you can probably use the virtual environment manager of your choice. Note pipenv sets the PYHTONPATH.  If you choose a different approach, you may have to handle that yourself.
```bash
cd src/hydro-evaluation
pipenv install
pipenv shell
```

Spin up the TimescaleDB in docker. Note, to load the full dataset in the example you will need about 20 GB of free space.
```bash
docker compose up -d
```

Apply database migrations
```bash
cd normalized_tags
alembic upgrade head
```

Load up some domains and crosswalk data
```bash
python add_domains.py
python load_geo_crosswalk.py
```

Load up some NWM and USGS data
```bash
python insert_nwm.py
python insert_usgs.py
```

Create joined view in DB
```bash
python create_joined_view.py
```

Calculate and view some metrics
```bash
python metrics.py
```
