# Setup a Sample Evaluation
As a test we will work with the HUC10 basins and all the USGS gage locations in the CONUS.

## Pangeo Docker Container
docker run -it --rm --volume $HOME:$HOME -p 8888:8888 pangeo/pangeo-notebook:latest jupyter lab --ip 0.0.0.0 $HOME

## Basin File
The HUC10 shapefile should be downloaded from the USGS [https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/WBD/National/GDB/]

`wget https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/WBD/National/GDB/WBD_National_GDB.zip`

This geodatabase contains many different HUC resolutions.  For this example we are using HUC10.  It must be manually subset to CONUS extent and save as shapefile and saved to `config.HUC10_SHP_FILEPATH`.

Run `basin_to_parquet.py` to convert to parquet file for faster access and querying.

## Weights File
In order to be able to quickly generate mean areal values for the HUC10 basins from gridded data (forcings/assim), a basin weights file is generated.

A `TEMPLATE_BLOB_NAME = "nwm.20221001/forcing_medium_range/nwm.t00z.medium_range.forcing.f001.conus.nc"` is specified in the file that is used to fetch a raster file for use as a template for mask/weights generation.

`wget https://storage.googleapis.com/national-water-model/nwm.20221001/forcing_medium_range/nwm.t00z.medium_range.forcing.f001.conus.nc`

Run `generate_weights.py` to generate the weights file for mean areal values.

## Route Link File
The route link file contains information about the NWM features as well as their related gage_id and location (lat/lon).

This file needs to be downloaded and placed in the cache folder at the location set as`ROUTE_LINK_FILE` in the `config` file.  

`wget https://www.nco.ncep.noaa.gov/pmb/codes/nwprod/nwm.v2.2.2/parm/domain/RouteLink_CONUS.nc`

Then `routing_to_parquet.py` must be run to convert it to a parquet file.

## Download Data
Use `grid_to_parquet.ipynb`, `nwm_to_parquet.ipynb` and `usgs_to_parquet.ipynb` to download NWM streamflow forecasts at USGS gage sites, USGS gage data at all USGS gage site used in NWM, forcing precipitation data aggregated to HUC10, assim precipitation aggregated to HUC10.

# Evaluate
