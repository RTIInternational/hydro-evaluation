# Load (lets rename this to load)
The data loading library is a set of tools that can be used to load data from various sources 
and save as parquet files for use in the system.

## NWM v2.2
The primary source for NWM data is the Google Cloud Bucket [https://console.cloud.google.com/storage/browser/national-water-model]
but other sources are supported that follow the same naming convention.

This package provides tools to fetch and pre-process NWM streamflow and forcing data

## NextGen
NextGen data is obtained from S3 buckets.

## Cache
The cache structure (initial):
```bash
.
├── geo
└── nwm_v22
    ├── nc
    ├── parquet
    └── zarr


```