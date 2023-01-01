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
