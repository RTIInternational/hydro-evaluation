create extension postgis;
create extension postgis_raster;

select * from route_links rl order by id desc limit 5;

WITH RECURSIVE c AS (
   SELECT 1 AS id
   UNION ALL
   SELECT sa.child_id
   FROM subject_associations AS sa
      JOIN c ON c.id = sa. parent_id
)
SELECT id FROM c;

WITH RECURSIVE search_tree AS (
    SELECT rl.link, rl.to_node, rl.geom
    FROM route_links rl
    where rl.link = 9642177
  UNION ALL
    SELECT rl.link, rl.to_node, rl.geom
    FROM route_links rl, search_tree st
    WHERE rl.link = st.to_node
)
SELECT * FROM search_tree;

CREATE SEQUENCE myview_vid_seq CYCLE;



CREATE OR REPLACE VIEW upstream AS 
	WITH RECURSIVE search_tree AS (
	    SELECT rl.nwm_feature_id, rl.to_node, rl.geom
	    FROM route_links rl
	    where rl.nwm_feature_id = 9630389
	  UNION ALL
	    SELECT rl.nwm_feature_id, rl.to_node, rl.geom
	    FROM route_links rl, search_tree st
	    WHERE rl.to_node = st.nwm_feature_id
	)
	SELECT row_number() over () AS vid, search_tree.* FROM search_tree;


CREATE OR REPLACE VIEW downstream AS 
	WITH RECURSIVE search_tree AS (
	    SELECT rl.nwm_feature_id, rl.to_node, rl.geom
	    FROM route_links rl
	    where rl.nwm_feature_id = 9630389
	  UNION ALL
	    SELECT rl.nwm_feature_id, rl.to_node, rl.geom
	    FROM route_links rl, search_tree st
	    WHERE rl.nwm_feature_id = st.to_node
	)
	SELECT row_number() over () AS vid, search_tree.* FROM search_tree;


CREATE OR REPLACE VIEW usgs_gages AS (
	SELECT * FROM route_links rl WHERE rl.gage_id IS NOT NULL AND rl.gage_id ~ '^[0-9\.]+$'
); 


SELECT * FROM route_links rl WHERE rl.gage_id IS NOT NULL AND rl.gage_id ~ '^[0-9\.]+$'


CREATE OR REPLACE FUNCTION get_upstream(_nwm_feature_id int)
  RETURNS TABLE (
    nwm_feature_id  int,
    to_node         int,
    geom            GEOMETRY(Point, 4326)
  )
  LANGUAGE plpgsql AS
$$
BEGIN
   RETURN QUERY
   WITH RECURSIVE search_tree AS (
	    SELECT rl.nwm_feature_id, rl.to_node, rl.geom
	    FROM route_links rl
	    where rl.nwm_feature_id = _nwm_feature_id
	  UNION ALL
	    SELECT rl.nwm_feature_id, rl.to_node, rl.geom
	    FROM route_links rl, search_tree st
	    WHERE rl.to_node = st.nwm_feature_id
	)
	SELECT * FROM search_tree;
END
$$;

SELECT * FROM get_upstream(9630389);

-- Try tiling query
WITH 
    bounds AS (
        SELECT ST_Segmentize(ST_MakeEnvelope(-8296780.798186153, 4970241.32721529, -8257645.039704142, 5009377.085697301, 3857),9783.939620502759) AS geom, 
               ST_Segmentize(ST_MakeEnvelope(-8296780.798186153, 4970241.32721529, -8257645.039704142, 5009377.085697301, 3857),9783.939620502759)::box2d AS b2d
    ),
    mvtgeom AS (
        SELECT ST_AsMVTGeom(ST_Transform(t.geom, '+proj=lcc +lat_0=40 +lon_0=-97 +lat_1=30 +lat_2=60 +x_0=0 +y_0=0 +R=6370000 +units=m +no_defs +type=crs'::TEXT, 3857), bounds.b2d) AS geom, 
               gid, huc10
        FROM huc10 t, bounds
        WHERE ST_Intersects(t.geom, ST_Transform(bounds.geom, 
        	'+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs +type=crs'::text, 
			'+proj=lcc +lat_0=40 +lon_0=-97 +lat_1=30 +lat_2=60 +x_0=0 +y_0=0 +R=6370000 +units=m +no_defs +type=crs'::TEXT)
			)
	    ) 
    SELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom;

   
SELECT ST_SRID(geom) FROM huc10_mean LIMIT 1;
   
   
-- create a huc map view
DROP VIEW huc10_mean;
CREATE VIEW huc10_mean AS (
	WITH medium_range_map AS (
	    SELECT
	        gid,huc10,reference_time,geom,lead_time,
	        (ST_SummaryStatsAgg(ST_Clip(rast, geom, true), 1, true)).mean as value
	    FROM
	        forcing_medium_range_attrs as raster
	    INNER JOIN huc10 on
	        ST_INTERSECTS(geom, rast)
	    GROUP BY
	        huc10,reference_time,geom,lead_time,gid
	    ORDER BY
	        reference_time,huc10
	)
	SELECT * FROM medium_range_map WHERE reference_time = '2022-10-01 00:00:00' AND lead_time = '1'
);

SELECT * FROM huc10_mean WHERE huc10 LIKE '0303%';

SELECT count(*) FROM huc10;


WITH 
    bounds AS (
        SELECT ST_Segmentize(ST_MakeEnvelope(-8218509.281222133, 5009377.085697301, -8140237.764258113, 5087648.602661321, 3857),19567.879241005052) AS geom, 
               ST_Segmentize(ST_MakeEnvelope(-8218509.281222133, 5009377.085697301, -8140237.764258113, 5087648.602661321, 3857),19567.879241005052)::box2d AS b2d
    ),
    mvtgeom AS (
        SELECT ST_AsMVTGeom(ST_Transform(
                t.geom, 
                '+proj=lcc +lat_0=40 +lon_0=-97 +lat_1=30 +lat_2=60 +x_0=0 +y_0=0 +R=6370000 +units=m +no_defs +type=crs'::TEXT, 
                3857
            ), 
        bounds.b2d) AS geom, 
               gid, huc10, value
        FROM huc10_mean t, bounds
        WHERE ST_Intersects(t.geom, ST_Transform(
            bounds.geom,
            '+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs +type=crs'::text, 
                        '+proj=lcc +lat_0=40 +lon_0=-97 +lat_1=30 +lat_2=60 +x_0=0 +y_0=0 +R=6370000 +units=m +no_defs +type=crs'::TEXT
            )
        )
    ) 
    SELECT ST_AsMVT(mvtgeom.*) FROM mvtgeom
            
            
