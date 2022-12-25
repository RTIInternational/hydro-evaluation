# Set environment variables for database connection
set PGHOST=localhost
set PGPORT=5432
set PGUSER=postgres
set PGPASSWORD=postgrespassword
set PGDATABASE=postgres

# Call the raster2pqsql utility
# raster2pgsql -C -F -d -t auto -I /home/matt/repos/hydro-evaluation/src/hydro-evaluation/grids/data/*.tiff grids.forcing_medium_range > out.sql

raster2pgsql -I -F -C -r -c -t auto /home/matt/repos/hydro-evaluation/src/hydro-evaluation/grids/data/nwm.20221001.forcing_medium_range.nwm.t00z.medium_range.forcing.f001.conus.tiff forcing_medium_range  > rast.sql
psql -U postgres -h localhost -f rast.sql 

# raster2pgsql -d /home/matt/repos/hydro-evaluation/src/hydro-evaluation/nwm_grid.tif grids.forcing_medium_range > out.sql

shp2pgsql -d -D -I /home/matt/huc10_lcc.shp public.huc10 > shp.sql
psql -U postgres -h localhost -f shp.sql 

