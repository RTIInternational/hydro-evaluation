Here we will test code related to collecting and processing the gridded forcing data.

For example: https://console.cloud.google.com/storage/browser/national-water-model/nwm.20180917/forcing_medium_range

There is a need to process mean areal values across these datasets to generate timeseries, for example the mean areal precipitation over a HUC10 for a forecast.

At a minimum I see that we will investigate two approaches:

- Load the grids into the database and calculate the mean values on the fly and/or via a materialized view.
- Precalculate the mean values and store those mean values in the database (or a file based system) as timeseries.