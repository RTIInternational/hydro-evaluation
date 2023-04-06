exploration
evaluation
visualization
analyze

water-explore-evaluate-visualize-analyze

CIROH evaluation and exploration tools (CHEET)

cheet
    load
        get_nwm_file_list(method: str) -> List[str]
            two approaches here - get file list from GCP bucket using filters, or generate using logic
        fetch_nwm(file_list: List[str]) -> df
        fetch_nwm_forcing(file_list: List[str], weights_file: str) -> df
        fetch_usgs() -> df
        fetch_ngen(bucket: str, path:str) -> df
    store.py
        save_to_cache(df, filepath)
        generate_cache_filepath() -> str
        cache, maybe a class to manage the storage
    query.py
        duckdb_metrics() -> df
            "standard" population type metrics are efficiently computed using SQL
        df_metrics() -> df
            "time dependent" metrics may be better suited to a different, more iterative calculation approach. Or possibly need an extension. Dask could be useful in these cases.
    viz.py
        functions that return hv and gv objects
        map()
        plot()
    utils.py
    const.py
        available_metrics
        fetching_constants

    config.py - maybe don't need this

# Cache Format
studies
    study_1
        geo
        nwm
        parquet
        zarr
    study_2
        geo
        nwm
        parquet
        zarr

