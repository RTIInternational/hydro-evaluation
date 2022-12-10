Getting Started (normalized_tags)

```bash
cd src/hydro-eval
pipenv install
pipenv shell
```

```bash
docker compose up -d
alembic upgrade head
```

```bash
python add_domains.py
```

```bash
python load/test_hydrotools.py
```

Getting started (wide_table)
