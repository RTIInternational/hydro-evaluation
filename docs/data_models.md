The `timeseries` data model (mostly taken from HydroTools) describes the schema used to store, you guessed it, timeseries data from both observed and simulated sources.  In the context of this system, this datamodel will be utilized as the format to store data as parquet files.  As such, a standard file directory structure is also important.  See [Cache Directory Structure] below.

`reference_time`: some reference time for a particular model simulation.
`value_time`: time a value
`value`: 
`variable_name`: 
`measurement_unit`: 
`configuration`: 
`series`: 
`geometry_id`: [string] should be globally unique and reference a geometric entity
`geometry`: [wkt or wkb] we could possibly store the geometry directly with the timeseries data.

In a traditional relational database we would aim for referential integrity, but considering that we are storing this data in parquet files, this is not viable. The "non-timeseries" tables could be generated as 

The `crosswalk` data model is used to define relationships between geometric.  This data would be good as a relational data table(s)



The `geometry` data model is used to store geographic entities.  This could include almost anything but in the context of hydrologic data we will mostly be referring to forecast points (points) and catchments (polygons), and possibly stream reaches (polylines).

`id`
`geometry`



# Cache Directory Structure
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
