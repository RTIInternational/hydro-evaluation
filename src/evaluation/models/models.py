from datetime import datetime
from typing import List, Optional, Union
from collections.abc import Iterable
from pydantic import ValidationError, validator
from enum import Enum

from pydantic import BaseModel as PydanticBaseModel

def is_iterable_not_str(obj):
    if isinstance(obj, Iterable) and not isinstance(obj, str ):
        return True
    return False
        
class BaseModel(PydanticBaseModel):
    class Config:
        arbitrary_types_allowed = True
        smart_union = True


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
    value: Union[str, int, float, datetime, List[Union[str, int, float, datetime]]]

    @validator('value')
    def in_operator_must_have_iterable(cls, v, values):
        if is_iterable_not_str(v) and values["operator"] != "in":
            raise ValueError("iterable value must be used with 'in' operator")
        
        if  values["operator"] == "in" and not is_iterable_not_str(v):
            raise ValueError("'in' operator can only be used with iterable value")
        
        return v

class MetricQuery(BaseModel):
    forecast_dir = str
    observed_dir = str
    group_by = List[MetricFilterFieldEnum]
    order_by = List[MetricFilterFieldEnum]
    filters = List[MetricFilter]

