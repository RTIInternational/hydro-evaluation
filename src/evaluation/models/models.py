from datetime import datetime
from typing import List, Optional, Union
from pydantic import BaseModel
from enum import Enum


class OperatorEnum(str, Enum):
    eq = "="
    gt = ">"
    lt = "<"
    gte = ">="
    lte = "<="
    islike = "like"
    isin = "in"


class MetricFilterFieldEnum(str, Enum):
    value_time = "value_time"
    reference_time = "reference_time"
    nwm_feature_id = "nwm_feature_id"
    forecast_value = "forecast_value"
    configuration = "configuration"  
    measurement_unit = "measurement_unit"
    variable_name = "variable_name"
    observed_value = "observed_value"
    usgs_site_code = "usgs_site_code"
    lead_time = "lead_time"


# class JoinedTable(BaseModel):
#     value_time,
#     reference_time,
#     nwm_feature_id,   
#     forecast_value, 
#     configuration,  
#     measurement_unit,     
#     variable_name,
#     observed_value,
#     usgs_site_code,
#     lead_time


# class TimeseriesTable(BaseModel):
#     value_time,
#     reference_time,
#     nwm_feature_id,   
#     forecast_value, 
#     configuration,  
#     measurement_unit,     
#     variable_name,
#     observed_value,
#     usgs_site_code,
#     lead_time
    

class MetricFilter(BaseModel):
    column: MetricFilterFieldEnum
    operator: OperatorEnum
    value: Union[str, float, int, datetime, List[str, float, int, datetime]]


class MetricQuery(BaseModel):
    forecast_dir = str
    observed_dir = str
    group_by = List[JoinedTableFieldEnum]
    order_by = List[JoinedTableFieldEnum]
    filters = List[MetricFilter]

