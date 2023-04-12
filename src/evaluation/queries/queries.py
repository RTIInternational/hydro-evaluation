import warnings
from collections.abc import Iterable
from datetime import datetime
from typing import List, Union

import config
from models import MetricFilter, MetricQuery

SQL_DATETIME_STR_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_datetime_list_string(values):
    return [f"'{v.strftime(SQL_DATETIME_STR_FORMAT)}'" for v in values]


def format_iterable_value(
        values: Iterable[Union[str, int, float, datetime]]
    ) -> str:
    """Returns an SQL formatted string from list of values. 
    
    Parameters
    ----------
    values : Iterable 
        Contains values to be formatted as a string for SQL. Only one type of 
        value (str, int, float, datetime) should be used. First value in list 
        is used to determine value type. Values are not checked for type 
        consistency.

    Returns
    -------
    formatted_string : str

    """

    # string
    if isinstance(values[0], str):
        return f"""({",".join([f"'{v}'" for v in values])})"""
    # int or float
    elif (
        isinstance(values[0], int) 
        or isinstance(values[0], float)
    ):
        return f"""({",".join([f"{v}" for v in values])})"""
    # datetime
    elif isinstance(values[0], datetime):
        return f"""({",".join(get_datetime_list_string(values))})"""
    else:
        warnings.warn("treating value as string because didn't know what else to do.")
        return f"""({",".join([f"'{str(v)}'" for v in values])})"""


def format_filter_item(filter: MetricFilter) -> str:
    """Returns an SQL formatted string for single filter object.

    Parameters
    ----------
    filter: models.MetricFilter
        A single MetricFilter object.

    Returns
    -------
    formatted_string : str
    
    """

    if isinstance(filter.value, str):
        return f"""{filter.column} {filter.operator} '{filter.value}'"""
    elif (
        isinstance(filter.value, int) 
        or isinstance(filter.value, float)
    ):
        return f"""{filter.column} {filter.operator} {filter.value}"""
    elif isinstance(filter.value, datetime):
        dt_str = filter.value.strftime(SQL_DATETIME_STR_FORMAT)
        return f"""{filter.column} {filter.operator} '{dt_str}'"""
    elif (
        isinstance(filter.value, Iterable) 
        and not isinstance(filter.value, str)
    ):
        value = format_iterable_value(filter.value)
        return f"""{filter.column} {filter.operator} {value}"""
    else:
        warnings.warn("treating value as string because didn't know what else to do.")
        return f"""{filter.column} {filter.operator} '{str(filter.value)}'"""


def format_filters(filters: List[MetricFilter]) -> List[str]:
    """Generate SQL where clause string from filters.

    Parameters
    ----------
    filters : List[MetricFilter]
        A list of MetricFilter objects describing the filters.

    Returns
    -------
    where_clause : str
        A where clause formatted string 
    """
    if len(filters) > 0:
        filter_strs = []
        for f in filters:
            filter_strs.append(format_filter_item(f))
        qry = f"""WHERE {f" AND ".join(filter_strs)}"""
        return qry

    return "--no where clause"
    

def calculate_nwm_feature_metrics(
    forecast_dir: str,
    observed_dir: str,
    group_by: List[str], 
    order_by: List[str], 
    filters: Union[List[dict], None] = None
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
    if filters == None:
        filters = []
        
    print({
            "forecast_dir": forecast_dir,
            "observed_dir": observed_dir,
            "group_by": group_by,
            "order_by": order_by,
            "filters": filters
        })
  
    metric_query = MetricQuery.parse_obj(
        {
            "forecast_dir": forecast_dir,
            "observed_dir": observed_dir,
            "group_by": group_by,
            "order_by": order_by,
            "filters": filters
        }
    )
    print(metric_query)

    # query =  f"""
    #     WITH joined as (
    #         SELECT 
    #             nd.reference_time,
    #             nd.value_time,
    #             nd.nwm_feature_id,   
    #             nd.value as forecast_value, 
    #             nd.configuration,  
    #             nd.measurement_unit,     
    #             nd.variable_name,
    #             ud.value as observed_value,
    #             ud.usgs_site_code,
    #             nd.value_time - nd.reference_time as lead_time
    #         FROM '{forecast_dir}/*.parquet' nd 
    #         JOIN '{config.ROUTE_LINK_PARQUET}' nux 
    #             on nux.nwm_feature_id = nd.nwm_feature_id 
    #         JOIN '{observed_dir}/*.parquet' ud 
    #             on nux.gage_id = ud.usgs_site_code 
    #             and nd.value_time = ud.value_time 
    #             and nd.measurement_unit = ud.measurement_unit
    #             and nd.variable_name = ud.variable_name
    #     )
    #     SELECT 
    #         {",".join(group_by)},
    #         regr_intercept(forecast_value, observed_value) as intercept,
    #         covar_pop(forecast_value, observed_value) as covariance,
    #         corr(forecast_value, observed_value) as corr,
    #         regr_r2(forecast_value, observed_value) as r_squared,
    #         count(forecast_value) as forecast_count,
    #         count(observed_value) as observed_count,
    #         avg(forecast_value) as forecast_average,
    #         avg(observed_value) as observed_average,
    #         var_pop(forecast_value) as forecast_variance,
    #         var_pop(observed_value) as observed_variance,
    #         max(forecast_value) - max(observed_value) as max_forecast_delta,
    #         sum(observed_value - forecast_value)/count(*) as bias
    #     FROM
    #         joined
    #     {format_filters(metric_query.filters)}
    #     GROUP BY
    #         {",".join(group_by)}
    #     ORDER BY 
    #         {",".join(order_by)}
    # ;"""
    # return query


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