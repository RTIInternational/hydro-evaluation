from typing import List

def format_filters(filters):
        return [f'{f["column"]} {f["operator"]} {f["value"]}' for f in filters]

def calculate_metrics(group_by: List[str], order_by: List[str], filters: List[dict]) -> str:
    """Generate a metrics query
    
    group_by = ["lead_time", "loc", "source"]
    order_by = ["source", "lead_time"]
    filters = [
        {
            "column": "source",
            "operator": "=",
            "value": "'NWM'"
        },
        {
            "column": "loc",
            "operator": "=",
            "value": "'CARO2'"
        },
        {
            "column": "lead_time",
            "operator": "<=",
            "value": "'10 days'"
        }
    ]
    
    """

    query = f"""
        WITH joined as (
            SELECT 
                nd.reference_time,
                nd.value_time,
                nd.nwm_feature_id,   
                nd.value as forecast_value, 
                nd.configuration,  
                nd.measurement_unit,     
                nd.variable_name,
                nux.geom as geom, 
                ud.value as observed_value,
                ud.usgs_site_code,
                nd.value_time - nd.reference_time as lead_time
            FROM nwm_data nd 
            JOIN nwm_usgs_xwalk nux on nux.nwm_feature_id = nd.nwm_feature_id 
            JOIN usgs_data ud 
                on nux.usgs_site_code  = ud.usgs_site_code 
                and nd.value_time = ud.value_time 
                and nd.measurement_unit = ud.measurement_unit
                and nd.variable_name = ud.variable_name
        ),
        t as (
            SELECT  
                {",".join(group_by)},
                stats_agg(forecast_value, observed_value) as stats2D,
                stats_agg(observed_value) as stats1D_Obs,
                stats_agg(forecast_value) as stats1D_Fcast,
                sum(observed_value - forecast_value)/count(*) as bias
            FROM
                joined
            WHERE 
                {" AND ".join(format_filters(filters))}
            GROUP BY
                {",".join(group_by)}
        )
        SELECT
            {",".join(group_by)},
            intercept(stats2D) as intercept,
            covariance(stats2D) as covariance,
            corr(stats2D) as corr,
            determination_coeff(stats2D) as r_squared,
            num_vals(stats1D_Fcast) as forecast_count,
            num_vals(stats1D_Obs) as observed_count,
            average(stats1D_Fcast) as forecast_average,
            average(stats1D_Obs) as observed_average,
            variance(stats1D_Fcast) as forecast_variance,
            variance(stats1D_Obs) as observed_variance,
            bias
        FROM t
        ORDER BY 
            {",".join(order_by)}
    ;"""

    return query
