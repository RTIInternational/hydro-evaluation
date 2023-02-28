from typing import List
import config


def format_filters(filters: List[dict]) -> List[str]:
    """Generate strings from filter dict.
    
   ToDo:  This is probably not robust enough.
    """
    filter_strs = []
    for f in filters:
        # print(f)
        if type(f["value"]) == str:
            filter_strs.append(f"""{f["column"]} {f["operator"]} '{f["value"]}'""")
        else:
            filter_strs.append(f"""{f["column"]} {f["operator"]} {f["value"]}""")
    return filter_strs
    

def calculate_nwm_feature_metrics(
    forecast_dir: str,
    observed_dir: str,
    group_by: List[str], 
    order_by: List[str], 
    filters: List[dict]
) -> str:
    """Generate a metrics query
    
    group_by = ["lead_time", "nwm_feature_id"]
    order_by = ["lead_time", "nwm_feature_id", "lead_time"]
    filters = [
        {
            "column": "nwm_feature_id",
            "operator": "=",
            "value": "'123456'"
        },
        {
            "column": "reference_time",
            "operator": "=",
            "value": "'2022-01-01 00:00'"
        },
        {
            "column": "lead_time",
            "operator": "<=",
            "value": "'10 days'"
        }
    ]
    
    """
    
    query =  f"""
        WITH joined as (
            SELECT 
                nd.reference_time,
                nd.value_time,
                nd.nwm_feature_id,   
                nd.value as forecast_value, 
                nd.configuration,  
                nd.measurement_unit,     
                nd.variable_name,
                ud.value as observed_value,
                ud.usgs_site_code,
                nd.value_time - nd.reference_time as lead_time
            FROM '{forecast_dir}/*.parquet' nd 
            JOIN '{config.ROUTE_LINK_PARQUET}' nux 
                on nux.nwm_feature_id = nd.nwm_feature_id 
            JOIN '{observed_dir}/*.parquet' ud 
                on nux.gage_id = ud.usgs_site_code 
                and nd.value_time = ud.value_time 
                and nd.measurement_unit = ud.measurement_unit
                and nd.variable_name = ud.variable_name
        )
        SELECT 
            {",".join(group_by)},
            regr_intercept(forecast_value, observed_value) as intercept,
            covar_pop(forecast_value, observed_value) as covariance,
            corr(forecast_value, observed_value) as corr,
            regr_r2(forecast_value, observed_value) as r_squared,
            count(forecast_value) as forecast_count,
            count(observed_value) as observed_count,
            avg(forecast_value) as forecast_average,
            avg(observed_value) as observed_average,
            var_pop(forecast_value) as forecast_variance,
            var_pop(observed_value) as observed_variance,
            max(forecast_value) - max(observed_value) as max_forecast_delta,
            sum(observed_value - forecast_value)/count(*) as bias
        FROM
            joined
        WHERE 
            {" AND ".join(format_filters(filters))}
        GROUP BY
            {",".join(group_by)}
        ORDER BY 
            {",".join(order_by)}
    ;"""
    return query


def get_joined_nwm_feature_timeseries(
    forecast_dir: str,
    observed_dir: str,
    filters: List[dict]
) -> str:
    """Fetch joined timeseries."""

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
                ud.value as observed_value,
                ud.usgs_site_code,
                nd.value_time - nd.reference_time as lead_time
            FROM '{forecast_dir}/*.parquet' nd 
            JOIN '{config.ROUTE_LINK_PARQUET}' nux 
                on nux.nwm_feature_id = nd.nwm_feature_id 
            JOIN '{observed_dir}/*.parquet' ud 
                on nux.gage_id = ud.usgs_site_code 
                and nd.value_time = ud.value_time 
                and nd.measurement_unit = ud.measurement_unit
                and nd.variable_name = ud.variable_name
        )
        SELECT 
            *
        FROM
            joined
        WHERE 
            {" AND ".join(format_filters(filters))}
    ;"""

    return query


def calculate_catchment_metrics(
    forecast_dir: str,
    observed_dir: str,
    group_by: List[str], 
    order_by: List[str], 
    filters: List[dict]
) -> str:
    """Generate a metrics query
    
    group_by = ["lead_time", "nwm_feature_id"]
    order_by = ["lead_time", "nwm_feature_id", "lead_time"]
    filters = [
        {
            "column": "nwm_feature_id",
            "operator": "=",
            "value": "'123456'"
        },
        {
            "column": "reference_time",
            "operator": "=",
            "value": "'2022-01-01 00:00'"
        },
        {
            "column": "lead_time",
            "operator": "<=",
            "value": "'10 days'"
        }
    ]
    
    """
    
    query =  f"""
        WITH joined as (
            SELECT 
                nd.reference_time,
                nd.value_time,
                nd.catchment_id,   
                nd.value as forecast_value, 
                nd.configuration,  
                nd.measurement_unit,     
                nd.variable_name,
                ud.value as observed_value,
                nd.value_time - nd.reference_time as lead_time
            FROM '{forecast_dir}/*.parquet' nd 
            JOIN '{observed_dir}/*.parquet' ud 
                on ud.catchment_id = nd.catchment_id
                and nd.value_time = ud.value_time 
                and nd.measurement_unit = ud.measurement_unit
                and nd.variable_name = ud.variable_name
        )
        SELECT 
            {",".join(group_by)},
            regr_intercept(forecast_value, observed_value) as intercept,
            covar_pop(forecast_value, observed_value) as covariance,
            corr(forecast_value, observed_value) as corr,
            regr_r2(forecast_value, observed_value) as r_squared,
            count(forecast_value) as forecast_count,
            count(observed_value) as observed_count,
            avg(forecast_value) as forecast_average,
            avg(observed_value) as observed_average,
            var_pop(forecast_value) as forecast_variance,
            var_pop(observed_value) as observed_variance,
            max(forecast_value) - max(observed_value) as max_forecast_delta,
            sum(observed_value - forecast_value)/count(*) as bias
        FROM
            joined
        WHERE 
            {" AND ".join(format_filters(filters))}
        GROUP BY
            {",".join(group_by)}
        ORDER BY 
            {",".join(order_by)}
    ;"""
    return query


def get_joined_catchment_timeseries(
    forecast_dir: str,
    observed_dir: str, 
    filters: List[dict]
) -> str:
    """Generate a metrics query
    
    filters = [
        {
            "column": "nwm_feature_id",
            "operator": "=",
            "value": "'123456'"
        },
        {
            "column": "reference_time",
            "operator": "=",
            "value": "'2022-01-01 00:00'"
        },
        {
            "column": "lead_time",
            "operator": "<=",
            "value": "'10 days'"
        }
    ]
    
    """
    
    query =  f"""
        WITH joined as (
            SELECT 
                nd.reference_time,
                nd.value_time,
                nd.catchment_id,   
                nd.value as forecast_value, 
                nd.configuration,  
                nd.measurement_unit,     
                nd.variable_name,
                ud.value as observed_value,
                nd.value_time - nd.reference_time as lead_time
            FROM '{forecast_dir}/*.parquet' nd 
            JOIN '{observed_dir}/*.parquet' ud 
                on ud.catchment_id = nd.catchment_id
                and nd.value_time = ud.value_time 
                and nd.measurement_unit = ud.measurement_unit
                and nd.variable_name = ud.variable_name
        )
        SELECT 
            *
        FROM
            joined
        WHERE 
            {" AND ".join(format_filters(filters))}
    ;"""
    return query