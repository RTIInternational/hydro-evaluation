Here we will test code related to collecting and processing the gridded forcing data.

For example: https://console.cloud.google.com/storage/browser/national-water-model/nwm.20180917/forcing_medium_range

There is a need to process mean areal values across these datasets to generate timeseries, for example the mean areal precipitation over a HUC10 for a forecast.

At a minimum I see that we will investigate two approaches:

- Load the grids into the database and calculate the mean values on the fly and/or via a materialized view.
- Precalculate the mean values and store those mean values in the database (or a file based system) as timeseries.

PROJ4
+proj=lcc +lat_0=40 +lon_0=-97 +lat_1=30 +lat_2=60 +x_0=0 +y_0=0 +R=6370000 +units=m +no_defs +type=crs
+proj=lcc +units=m +a=6370000.0 +b=6370000.0 +lat_1=30.0 +lat_2=60.0 +lat_0=40.0 +lon_0=-97.0 +x_0=0 +y_0=0 +k_0=1.0 +nadgrids=@null +wktext  +no_defs

