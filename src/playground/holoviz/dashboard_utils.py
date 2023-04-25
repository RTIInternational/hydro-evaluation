import sys
sys.path.insert(0, '../../')
sys.path.insert(0, '../../evaluation/')

import teehr.queries.duckdb as tqd
import duckdb as ddb
import temp_queries
import pandas as pd
import panel as pn
import geopandas as gpd
import numpy as np
from datetime import datetime, timedelta
from evaluation import utils, config
from typing import List, Union
import pathlib

import holoviews as hv

'''
data processing and misc utilities for TEEHR dashboards
'''

def get_comparison_metrics(
    primary_filepath: pathlib.Path,
    secondary_filepath: pathlib.Path,
    crosswalk_filepath: pathlib.Path,
    geometry_filepath: Union[pathlib.Path, None] = None,
    single_reference_time: Union[pd.Timestamp, None] = None,
    start_reference_time: Union[pd.Timestamp, None] = None,
    end_reference_time: Union[pd.Timestamp, None] = None,
    start_value_time: Union[pd.Timestamp, None] = None,
    end_value_time: Union[pd.Timestamp, None] = None,
    location_like: Union[str, None] = None,
    query_value_min: Union[float, None] = None,
    query_value_max: Union[float, None] = None,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:

    # set the geometry flag
    geom_flag = True
    if geometry_filepath == None:
        geom_flag = False
        
    # initialize group_by and order_by lists
    group_by_list=["primary_location_id","measurement_unit"]
    order_by_list=["primary_location_id"]

    filters=[]
    # build the filters
    
    # reference times
    if single_reference_time != None:
        group_by_list = group_by_list + ["reference_time"]
        order_by_list = order_by_list + ["reference_time"]
        filters.append(
            {
                "column": "reference_time",
                "operator": "=",
                "value": f"{single_reference_time}"
            }
        )
    elif start_reference_time != None:
        group_by_list = group_by_list + ["reference_time"]
        order_by_list = order_by_list + ["reference_time"]
        filters.append(
            {
                "column": "reference_time",
                "operator": ">=",
                "value": f"{start_reference_time}"
            }
        )
        if end_reference_time != None:
            filters.append(
                {
                    "column": "reference_time",
                    "operator": "<=",
                    "value": f"{end_reference_time}"
                } 
            )
        else:             
            print('set up an error catch here')
        
    # value times
    if start_value_time != None:
        group_by_list = group_by_list + ["value_time"]
        order_by_list = order_by_list + ["value_time"]
        filters.append(
            {
                "column": "value_time",
                "operator": ">=",
                "value": f"{start_value_time}"
            }
        )
        if end_value_time != None:
            filters.append(
                {
                    "column": "value_time",
                    "operator": "<=",
                    "value": f"{end_value_time}"
                } 
            )
        else:             
            print('set up an error catch here')    

    # location
    if location_like != None:
        group_by_list = group_by_list + ["primary_location_id"]
        order_by_list = order_by_list + ["primary_location_id"]
        filters.append(
            {
                "column": f"{primary_location_id}",
                "operator": "like",
                "value": f"{location_like}%"
            }
        )
        
    # min/max values --  ***  will eventually need to allow requiring one or other (primary or secondary) to be above threshold
    if query_value_min is not None:
        filters.append(
            {
                "column": "primary_value",
                "operator": ">=",
                "value": query_value_min
            }
        )
        filters.append(
            {
                "column": "secondary_value",
                "operator": ">=",
                "value": query_value_min
            }
        )
    if query_value_max is not None:
        filters.append(
            {
                "column": "primary_value",
                "operator": "<=",
                "value": query_value_max
            }
        )
        filters.append(
            {
                "column": "secondary_value",
                "operator": "<=",
                "value": query_value_max
            }
        )
            
    # get metrics for this reference time
    gdf = tqd.get_metrics(
        primary_filepath,
        secondary_filepath,
        crosswalk_filepath,
        group_by=group_by_list,
        order_by=order_by_list,
        filters=filters,
        return_query=False,
        geometry_filepath=geometry_filepath,       
        include_geometry=geom_flag,
    )            
        
    return gdf 


def merge_df_with_gdf(
    gdf, 
    geom_id_header: str, 
    df,
    location_id_header: str, 
) -> gpd.GeoDataFrame:
    '''
    merge data df (result of DDB query) with geometry, return a geodataframe
    '''
    # merge df with geodataframe
    merged_gdf = gdf.merge(df, left_on=geom_id_header, right_on=location_id_header)    

    # if IDs are HUC codes, convert to type 'category'
    if any(s in location_id_header for s in ["HUC","huc"]):
        print(f"converting column {location_id_header} to category")
        merged_gdf[location_id_header] = merged_gdf[location_id_header].astype("category")
    
    return merged_gdf

def get_parquet_date_range(source) -> pd.Timestamp:
    '''
    Query parquet files for defined fileset (source) and
    return the min/max value_times in the files
    '''    
    query = f"""
        SELECT count(distinct(value_time)) as count,
        min(value_time) as start_time,
        max(value_time) as end_time
        FROM read_parquet('{source}')
    ;"""
    df = ddb.query(query).to_df()
    return df.start_time[0], df.end_time[0]


def get_parquet_date_range_across_sources(source_list) -> pd.Timestamp:
    for source in source_list:
        source_start_date, source_end_date = get_parquet_date_range(source)
        if source == source_list[0]:
            start_date = source_start_date
            end_date = source_end_date
        else:
            if source_start_date > start_date:
                start_date = source_start_date
            if source_end_date < end_date:
                end_date = source_end_date
    return start_date, end_date


def get_reference_times():
    query = f"""select distinct(reference_time)as time
        from '{config.MEDIUM_RANGE_FORCING_PARQUET}/*.parquet'
        order by reference_time asc"""
    df = ddb.query(query).to_df()
    times = df.time.tolist()
    return times



def combine_attributes(filelist, viz_units):
    
    # note that any locations NOT included in all selected attribute tables will be excluded
    for file in filelist:
        df = pd.read_parquet(file)
        attribute_name = df['attribute_name'].iloc[0]
        attribute_units = df['attribute_units'].iloc[0]
        if attribute_units != 'none':
            df = convert_attr_to_viz_units(df, viz_units)      
        df = df.rename(columns = {'attribute_value': attribute_name + '_value', 
                                      'attribute_units': attribute_name + '_units'})
        df = df.drop('attribute_name', axis=1)
        if file == filelist[0]:
            attr_df = df.copy()
        else:
            attr_df = attr_df.merge(df, left_on='location_id',right_on='location_id')
            
    return attr_df

    
def merge_attr_to_gdf(
    gdf: gpd.GeoDataFrame, 
    attr_df: pd.DataFrame
)-> gpd.GeoDataFrame:
    
    value_columns = [col for col in attr_df.columns if col.find('value')>=0]
    gdf = gdf.merge(attr_df[['location_id'] + value_columns], 
                     left_on='primary_location_id', right_on='location_id')
    gdf = gdf.drop('location_id', axis=1)
    
    return gdf
    
def convert_area_to_ft2(units: str, values: pd.Series) -> pd.Series:
    if units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2) * (3.28**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values * (3.28**2)
    elif units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values
    return converted_values

def convert_area_to_mi2(units: str, values: pd.Series) -> pd.Series:
    if units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2) * (3.28**2) / (5280**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values * (3.28**2) / (5280**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (5280**2)
    elif units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values
    return converted_values
    
def convert_flow_to_cfs(units: str, values: pd.Series) -> pd.Series:
    if units in ['cms','m3/s']:
        converted_values = values * (3.28**3)
    elif units in ['cfs','ft3/s']:
        converted_values = values
    return converted_values 

def convert_area_to_m2(units: str, values: pd.Series) -> pd.Series:
    if units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2) / (3.28**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (3.28**2)
    elif units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values
    return converted_values

def convert_area_to_km2(units: str, values: pd.Series) -> pd.Series:
    if units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2) / (3.28**2) / (1000**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (3.28**2) / (1000**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values / (1000**2)        
    elif units in ['km2','sqkm','km**2','km^2']:
        converted_values = values
    return converted_values
    
def convert_flow_to_cms(units: str, values: pd.Series) -> pd.Series:
    if units in ['cfs','ft3/s']:
        converted_values = values / (3.28**3)
    elif units in ['cms','m3/s']:
        converted_values = values
    return converted_values 

        
def convert_metrics_to_viz_units(gdf: gpd.GeoDataFrame, viz_units: 'str') -> gpd.GeoDataFrame:
    
    measurement_unit = gdf['measurement_unit'][0]
    if viz_units == 'english':
        for col in gdf.columns:
            if col in ['primary_average','primary_max','primary_min','primary_sum',
                       'secondary_average','secondary_max','secondary_min','secondary_sum']:
                gdf[col] = convert_flow_to_cfs(measurement_unit, gdf[col])
        gdf['measurement_unit'] = 'ft3/s'
    elif viz_units == 'metric':
        for col in gdf.columns:
            if col.split("_")[0] in ['primary','secondary']:
                gdf[col] = convert_flow_to_cms(measurement_unit, gdf[col])  
        gdf['measurement_unit'] = 'm3/s'
        
    return gdf
                    
    
def convert_attr_to_viz_units(df: pd.DataFrame, viz_units: 'str') -> pd.DataFrame:
    
    attr_units = df['attribute_units'][0]
    attr_name = df['attribute_name'][0]
    
    if viz_units == 'english':
        if attr_name.find('area')>=0:
            df['attribute_value'] = convert_area_to_mi2(attr_units, df['attribute_value'])
            df['attribute_units'] = 'mi2'
        elif attr_name.find('flow')>=0:
            df['attribute_value'] = convert_area_to_cfs(attr_units, df['attribute_value'])
            df['attribute_units'] = 'cfs'   
            
    elif viz_units == 'metric':
        if attr_name.find('area')>=0:
            df['attribute_value'] = convert_area_to_km2(attr_units, df['attribute_value'])
            df['attribute_units'] = 'km2'
        elif attr_name.find('flow')>=0:
            df['attribute_value'] = convert_area_to_cms(attr_units, df['attribute_value'])
            df['attribute_units'] = 'cms'   
        
    return df


########## Widgets for dashboards

def get_event_date_range_slider(start_date, end_date, opts = {}):  
    '''
    Date range slider to select start and end dates of the event
    '''
    event_dates_slider = pn.widgets.DatetimeRangeSlider(
        name='Selected event start/end dates',
        start=start_date, 
        end=end_date,
        value=(start_date, end_date),
        step=1000*60*60,
        bar_color = "green",
        width_policy="fit",
        **opts,
    )
    return event_dates_slider


def get_event_dates_text(min_date, max_date):
    return pn.pane.HTML(f"Min/Max event dates available: {min_date} - {max_date}", 
                        sizing_mode = "stretch_width", 
                        style={'font-size': '15px', 'font-weight': 'bold'})


def get_huc2_selector():
    '''
    HUC2 region to explore, enables smaller region for faster responsiveness
    '''
    hucs=[
        "all",
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
    ]
    huc2_selector = pn.widgets.Select(name='HUC2', options=hucs, value="all", width_policy="fit")
    return huc2_selector       


def get_precip_char_measure_selector():
    measures = [
        "sum",
        "max",
        "min",
        "mean",
    ]  
    precip_measure_selector = pn.widgets.Select(name='Measure', options=measures, value=measures[0], width_policy="fit") 
    return precip_measure_selector


def get_flow_char_measure_selector():
    measures = [
        "sum",
        "max",
        "min",
        "mean"
    ]  
    flow_measure_selector = pn.widgets.Select(name='Measure', options=measures, value=measures[1], width_policy="fit") 
    return flow_measure_selector


def get_reference_time_player_all_dates():
    reference_times = get_reference_times()
    reference_time_player = pn.widgets.DiscretePlayer(name='Discrete Player', 
                                                      options=list(reference_times), 
                                                      value=reference_times[0], 
                                                      interval=5000)   
    return reference_time_player


def get_reference_time_player_selected_dates(
    start: pd.Timestamp = None,
    end: pd.Timestamp = None,
    opts: dict = {},
):
    reference_times = get_reference_times()
    reference_times_in_event = [t for t in reference_times if t >= start and t <= end]
    reference_time_player = pn.widgets.DiscretePlayer(name='Discrete Player', 
                                                      options=list(reference_times_in_event), 
                                                      value=reference_times_in_event[0], 
                                                      interval=5000,
                                                      show_loop_controls = False,
                                                      width_policy="fit",
                                                      **opts,
                                                      )   
    return reference_time_player


def get_reference_time(reference_time):
    return pn.pane.HTML(f"Current Reference Time:   {reference_time}", 
                        sizing_mode = "stretch_width", 
                        style={'font-size': '15px', 'font-weight': 'bold'})