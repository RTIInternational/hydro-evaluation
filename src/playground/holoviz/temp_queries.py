from typing import List
import config
import pandas as pd
from typing import List
import queries

def get_historical_filters(
    source: str, 
    location_id_header: str,
    location_id_like_string: str, 
    start_value_time: pd.Timestamp = None,
    end_value_time: pd.Timestamp = None,
    exclude_negative_values = True,
) -> dict:
    '''
    Build filter portion of query to extract historical timeseries by region 
    (portion of ID) and value_time range
    '''
    filters = []
    if location_id_like_string != "all":
        filters.append(
            {
                "column": f"{location_id_header}",
                "operator": "like",
                "value": f"{location_id_like_string}%"
            }
        )
    else:
        filters.append(
            {
                "column": f"{location_id_header}",
                "operator": "<>",
                "value": ""
            }
        )
    if start_value_time is not None:
        filters.append(
            {
                "column": "value_time",
                "operator": ">=",
                "value": f"{start_value_time}"
            }  
        )
    if end_value_time is not None:
        filters.append(
            {
                "column": "value_time",
                "operator": "<=",
                "value": f"{end_value_time}"
            }  
        )
    if exclude_negative_values:
        filters.append(
            {
                "column": "value",
                "operator": ">=",
                "value": 0
            }  
        )
    return filters


def get_historical_timeseries_data_query(
    source: str, 
    location_id_header: str, 
    filters: List[dict]
) -> str:    
    '''
    Build SQL query to extract historical timeseries by region 
    (portion of HUC ID) and value_time range
    ''' 
    query = f"""
        SELECT 
            *
        FROM read_parquet('{source}/*.parquet')
        WHERE 
            {" AND ".join(queries.format_filters(filters))}
        ORDER BY
            "{location_id_header}", value_time
    ;"""
    return query


def get_historical_timeseries_chars_query(
    source: str, 
    group_by: List[str],
    order_by: List[str],
    filters: List[dict]
) -> str:    
    '''
    Build SQL query to extract characteristics of timeseries within
    defined value_time range by region (portion of HUC ID) 
    '''    
    query = f"""
        SELECT 
            {",".join(group_by)},
            sum(value) as sum,
            max(value) as max,
            min(value) as min,
            mean(value) as mean,
            var_pop(value) as variance,
            any_value(measurement_unit) as units
        FROM read_parquet('{source}/*.parquet')
        WHERE 
            {" AND ".join(queries.format_filters(filters))}
        GROUP BY
            {",".join(group_by)}
        ORDER BY 
            {",".join(order_by)}
    ;"""
    return query