This document contains a brief description of some query responses generated from the NWM and USGS data stored in a PostgreSQL database (using TimescaleDB and PostGIS).  This uses the "wide_table" format for storing the data in the database, with compression enabled and applied to the `nwm_data` and `usgs_data` tables.

# Queries
For reference, the query definitions are:

```python
query_1 = queries.calculate_metrics(
    group_by=["reference_time", "nwm_feature_id"],
    order_by=["nwm_feature_id", "max_forecast_delta"],
    filters=[
        {
            "column": "nwm_feature_id",
            "operator": "in",
            "value": "(6731199,2441678,14586327,8573705,2567762,41002752,8268521,41026212,4709060,20957306)"
        }
    ]
)
```

```python
query_2 = queries.calculate_metrics(
    group_by=["reference_time", "nwm_feature_id", "value_time",],
    order_by=["nwm_feature_id", "value_time"],
    filters=[
        {
            "column": "nwm_feature_id",
            "operator": "=",
            "value": "17003262"
        }
    ]
)
```

```python
query_3 = queries.get_raw_timeseries(
    filters=[
        {
            "column": "nwm_feature_id",
            "operator": "=",
            "value": "17003262"
        }
    ]
)
```

# Query speed 
Note this is with compression enabled and applied.  Uncompressed data queries are a fair bit faster (maybe 2x ish). However, compression reduces the storage footprint from around 40 GB to around 1 GB, so it may be worth the performance hit to have it compressed.

Query 1: 1.5 sec.

Query 2: 0.4 sec.

Query 3: 0.5 sec.