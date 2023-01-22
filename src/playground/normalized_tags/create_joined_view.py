import os
import pandas as pd
import psycopg2
import psycopg2.extras
import pandas as pd
import os
import wide_table.config as config


def create_joined_view():
    conn = psycopg2.connect(config.CONNECTION)

    with conn:
        with conn.cursor() as cursor:
            try:
                query = """
                    CREATE VIEW joined_view AS (
                        WITH forecast AS (
                            SELECT 
                                datetime AS value_time, 
                                v.value AS value,
                                f1.value AS nwm_feature_id,
                                f2.value AS "configuration",
                                f3.value AS variable_name,
                                f4.value AS reference_time,
                                datetime - f4.value AS lead_time
                            FROM "values" v 
                            JOIN timeseries t ON v.timeseries_id = t.id 
                            JOIN timeseries_string_tag f1_tst ON v.timeseries_id = f1_tst.timeseries_id 
                                JOIN string_tags f1 ON f1_tst.string_tag_id = f1.id AND f1.string_tag_type_name = 'nwm_feature_id'
                            JOIN timeseries_string_tag f2_tst ON v.timeseries_id = f2_tst.timeseries_id 
                                JOIN string_tags f2 ON f2_tst.string_tag_id = f2.id AND f2.string_tag_type_name = 'configuration'
                            JOIN timeseries_string_tag f3_tst ON v.timeseries_id = f3_tst.timeseries_id 
                                JOIN string_tags f3 ON f3_tst.string_tag_id = f3.id AND f3.string_tag_type_name = 'variable_name'
                            JOIN timeseries_datetime_tag f4_tdt ON v.timeseries_id = f4_tdt.timeseries_id 
                                JOIN datetime_tags f4 ON f4_tdt.datetime_tag_id = f4.id AND f4.datetime_tag_type_name = 'reference_time'
                        ), observed AS (
                            SELECT 
                                datetime AS value_time, 
                                v.value AS value,
                                f1.value AS usgs_site_code,
                                f2.value AS "configuration",
                                f3.value AS variable_name
                            FROM "values" v 
                            JOIN timeseries t ON v.timeseries_id = t.id 
                            JOIN timeseries_string_tag f1_tst ON v.timeseries_id = f1_tst.timeseries_id 
                                JOIN string_tags f1 ON f1_tst.string_tag_id = f1.id AND f1.string_tag_type_name = 'usgs_site_code'
                            JOIN timeseries_string_tag f2_tst ON v.timeseries_id = f2_tst.timeseries_id 
                                JOIN string_tags f2 ON f2_tst.string_tag_id = f2.id AND f2.string_tag_type_name = 'configuration'
                            JOIN timeseries_string_tag f3_tst ON v.timeseries_id = f3_tst.timeseries_id 
                                JOIN string_tags f3 ON f3_tst.string_tag_id = f3.id AND f3.string_tag_type_name = 'variable_name'
                        )
                        SELECT
                            f.lead_time,
                            f.nwm_feature_id,
                            o.usgs_site_code,
                            f.reference_time,
                            f."configuration" AS forecast_configuration,
                            f.variable_name,
                            f.value as forecast_value,
                            o.value as observed_value,
                            o.value_time,
                            nux.geom 
                        FROM
                            forecast f
                        JOIN
                            nwm_usgs_xwalk nux ON nux.nwm_feature_id::TEXT = f.nwm_feature_id::TEXT 
                        INNER JOIN 
                            observed o
                        ON 
                            nux.usgs_site_code = o.usgs_site_code AND
                            f.value_time = o.value_time AND 
                            f.variable_name = o.variable_name
                    );
                """
                cursor.execute(query)

                conn.commit()
            except (Exception, psycopg2.Error) as error:
                print(error)

if __name__ == "__main__":
    create_joined_view()