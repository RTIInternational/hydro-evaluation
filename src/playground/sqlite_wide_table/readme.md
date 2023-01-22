To get started with the Wide Table approach.

Getting Started (wide_table)

Get your Python virtual environment setup.  We use pipenv, but you can probably use the virtual environment manager of your choice. Note pipenv sets the PYTHONPATH.  If you choose a different approach, you may have to handle that yourself.
```bash
cd src/hydro-evaluation
pipenv install
pipenv shell
```

Spin up the TimescaleDB in docker. Note, to load the full dataset in the example you will need about *100 GB* of free space.
```bash
docker compose up -d
```

Apply database migrations
```bash
cd wide_table
alembic upgrade head
```

Load up crosswalk data
```bash
python load_geo_crosswalk.py
```

Load up some NWM and USGS data
```bash
python insert_nwm.py
python insert_usgs.py
```

Calculate and view some metrics
```bash
python metrics.py
```
