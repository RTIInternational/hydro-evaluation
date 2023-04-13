# Timeseries Protocol

## Problem statement
We need an in memory timeseries format for moving timeseries between the ingest from various external sources and the database.

The solution must be fast and efficient and must accommodate datetime/values pairs along with metadata

For example, a json representation of what is required:

```json
"metadata": [
    {"variable": "flow"},
    {"units": "cubic feet/second"},
    {"location": "ABCD5"},
    {"reference-time": ISO8601}
],
"data": [
    {"datetime":2000-01-01T12:00:00Z, "value": 123}
    {"datetime":2000-01-02T12:00:00Z, "value": 125}
]

```

Formats to investigate:
* HDF5
* XArray
* Protobuf
* Parquet
* Pandas Series

Ease of use might be more important than performance.  Need to do some tests.
## Proposal
Raw source -> transfer protocol -> database storage -> SQL query/aggregate/visualize -> Python library 

We need to set up an example data pipeline that we can use to test performance

Raw NetCFD files pull both normal timeseries and mean areal timeseries (explore Zarr)
Put it in a transport protocol (explore)
Create a basic database schema (explore)
Load data into the database
Investigate different patterns and formats

Profile!

Prefect.


Use XArray as a transport protocol from various ingest functions to database loading.


## Other considerations
Pre-calculate MAP?

https://hakibenita.com/fast-load-data-python-postgresql


