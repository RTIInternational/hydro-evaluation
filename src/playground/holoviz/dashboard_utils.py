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
from pathlib import Path

import holoviews as hv

'''
misc utilities for TEEHR dashboards
''' 
    
def get_comparison_metrics(
    primary_filepath: Path,
    secondary_filepath: Path,
    crosswalk_filepath: Path,
    geometry_filepath: Union[Path, None] = None,
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,
    location_id: Union[str, List[str], None] = None,
    query_value_min: Union[float, None] = None,
    query_value_max: Union[float, None] = None,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:

    # start message
    print('Executing TEEHR query...')
    
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
    if reference_time_single != None:
        group_by_list = group_by_list + ["reference_time"]
        order_by_list = order_by_list + ["reference_time"]
        filters.append(
            {
                "column": "reference_time",
                "operator": "=",
                "value": f"{reference_time_single}"
            }
        )
    elif reference_time_start != None:
        group_by_list = group_by_list + ["reference_time"]
        order_by_list = order_by_list + ["reference_time"]
        filters.append(
            {
                "column": "reference_time",
                "operator": ">=",
                "value": f"{reference_time_start}"
            }
        )
        if reference_time_end != None:
            filters.append(
                {
                    "column": "reference_time",
                    "operator": "<=",
                    "value": f"{reference_time_end}"
                } 
            )
        else:             
            print('set up an error catch here')
        
    # value times
    if value_time_start != None:
        filters.append(
            {
                "column": "value_time",
                "operator": ">=",
                "value": f"{value_time_start}"
            }
        )
        if value_time_end != None:
            filters.append(
                {
                    "column": "value_time",
                    "operator": "<=",
                    "value": f"{value_time_end}"
                } 
            )
        else:             
            print('set up an error catch here')    

    # location
    if location_id != None:
        
        if type(location_id) == str:
            if location_id != 'ALL':
                filters.append(
                    {
                        "column": "primary_location_id",
                        "operator": "like",
                        "value": f"{location_id}%"
                    }
                )
        elif type(location_id) == list:
            filters.append(
                {
                    "column": "primary_location_id",
                    "operator": "in",
                    "value": location_id
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
    
    if type(gdf) in [gpd.GeoDataFrame, pd.DataFrame]:
        print(f"TEEHR query complete for {gdf['primary_location_id'].nunique()} locations and {gdf['reference_time'].nunique()} forecast reference times")
        
    return gdf


def get_parquet_value_time_range(source) -> List[pd.Timestamp]:
    '''
    Query parquet files for defined fileset (source directory) and
    return the min/max value_times in the files
    '''    
    query = f"""
        SELECT count(distinct(value_time)) as count,
        min(value_time) as start_time,
        max(value_time) as end_time
        FROM read_parquet('{source}')
    ;"""
    df = ddb.query(query).to_df()
    return [df.start_time[0], df.end_time[0]]

def get_parquet_reference_time_range(source: Path) -> List[pd.Timestamp]:
    '''
    Query parquet files for defined fileset (source directory) and
    return the min/max reference times in the files
    '''    
    query = f"""
        SELECT count(distinct(reference_time)) as count,
        min(reference_time) as start_time,
        max(reference_time) as end_time
        FROM read_parquet('{source}')
    ;"""
    df = ddb.query(query).to_df()
    return [df.start_time[0], df.end_time[0]] 
                              

def get_parquet_date_range_across_scenarios(
    pathlist: List[Path] = [],
    date_type: str = "value_time",
) -> List[pd.Timestamp]:
        
        
    print(f"Checking {date_type} range in the parquet files")    
      
    for source in pathlist:
        
        if date_type == "reference_time":
            [source_start_date, source_end_date] = get_parquet_reference_time_range(source)
        else:
            [source_start_date, source_end_date] = get_parquet_value_time_range(source)            
            
        if source == pathlist[0]:
            start_date = source_start_date
            end_date = source_end_date
        else:
            if type(source_start_date) == pd.Timestamp and type(start_date) == pd.Timestamp:
                if source_start_date > start_date:
                    start_date = source_start_date
            elif type(source_start_date) == pd.Timestamp and type(start_date) != pd.Timestamp:
                start_date = source_start_date
                
            if type(source_end_date) == pd.Timestamp and type(end_date) == pd.Timestamp:                    
                if source_end_date < end_date:
                    end_date = source_end_date
            elif type(source_end_date) == pd.Timestamp and type(end_date) != pd.Timestamp:
                end_date = source_end_date
                
    return [start_date, end_date]


def get_minmax_dates_text(
    min_date: pd.Timestamp, 
    max_date: pd.Timestamp,
    date_type: str,
    opts: dict = {},
) -> pn.pane:
    return pn.pane.HTML(f"Range of {date_type} available in cache: {min_date} - {max_date}", 
                        sizing_mode = "stretch_width",
                        style={'font-size': '15px', 'font-weight': 'bold'})

    
def get_parquet_reference_time_list(
    source: Path
) -> List[pd.Timestamp]:
    
    query = f"""select distinct(reference_time)as time
        from '{source}'
        order by reference_time asc"""
    df = ddb.query(query).to_df()
    times = df.time.tolist()
    return times

def get_huc_location_list(
    huc_level: int,
    huc_id: str, 
    attributes: dict
) -> List[str]:
    
    location_list=[]
    if huc_id != 'ALL' and "USGS_HUC10_CROSSWALK_FILEPATH" in attributes.keys():
        df = pd.read_parquet(attributes["USGS_HUC10_CROSSWALK_FILEPATH"])
        df['huc'] = df['secondary_location_id'].str[6:6+huc_level]
        location_list = df[df['huc']==huc_id]['primary_location_id'].to_list()
    else:
        location_list = huc_id
        
    return location_list


def get_scenario(
    scenario_definitions: List[dict],
    scenario_name: str,
    variable: str,
) -> dict:
    
    for scenario in scenario_definitions:
        if scenario["scenario_name"] == scenario_name and scenario["variable"] == variable:
            return scenario
        
    return {}


def get_parquet_pathlist_from_scenario(
    scenario_definitions: List[dict],
    scenario_name: str,
    variable: str,
) -> List[Path]:
    
    pathlist=[]
    for scenario in scenario_definitions:
        if scenario["scenario_name"] == scenario_name and scenario["variable"] == variable:
            pathlist.append(scenario["PRIMARY_FILEPATH"])
            pathlist.append(scenario["SECONDARY_FILEPATH"])

    return list(set(pathlist))


def get_date_range_slider_with_range_as_title(
    pathlist: List[Path] = [],
    date_type: str = "value_time",
    opts: dict = {}
) -> pn.Column:
    '''
    Date range slider to select start and end dates of the event
    '''
    
    if date_type == "reference_time":
        range_start, range_end = get_parquet_date_range_across_scenarios(pathlist, date_type = "reference_time")  
        value_end = range_start+timedelta(days=10)
    else:
        range_start, range_end = get_parquet_date_range_across_scenarios(pathlist, date_type = "value_time")  
        value_end = range_end
        
    dates_text = get_minmax_dates_text(range_start, range_end, date_type)
    dates_slider = pn.widgets.DatetimeRangeSlider(
        name='Selected start/end dates for evaluation',
        start=range_start, 
        end=range_end,
        value=(range_start, value_end),
        **opts,
    )
    return pn.Column(dates_text, dates_slider)



def get_huc2_selector() -> pn.widgets.Select:
    '''
    HUC2 region to explore, enables smaller region for faster responsiveness
    '''
    hucs=[
        "ALL",
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
    huc2_selector = pn.widgets.Select(name='HUC-2 Subregion', options=hucs, value="all", width_policy="fit")
    return huc2_selector       


def get_variable_selector(variable_list: List[str]) -> pn.widgets.Select:
    
    variable_selector = pn.widgets.Select(name='Evaluation variable', 
                                          options=variable_list, 
                                          value=variable_list[0], 
                                          width_policy="fit")
    
    return variable_selector


def get_scenario_variables(scenario_definitions: List[dict]) -> List[str]:
    
    variable_list = []
    for scenario in scenario_definitions:
        variable_list.append(scenario["variable"])
    
    return list(set(variable_list))


def get_scenario_selector(scenario_name_list: List[str]) -> pn.widgets.Select:
    
    scenario_selector = pn.widgets.Select(name='Evaluation scenario', 
                                          options=scenario_name_list, 
                                          value=scenario_name_list[0], 
                                          width_policy="fit")
    
    return scenario_selector


def get_scenario_names(scenario_definitions: List[dict]) -> List[str]:
    
    scenario_name_list = []
    for scenario in scenario_definitions:
        scenario_name_list.append(scenario["scenario_name"])
    
    return list(set(scenario_name_list))
    

def get_precip_char_measure_selector() -> pn.widgets.Select:
    measures = [
        "sum",
        "max",
        "min",
        "mean",
    ]  
    precip_measure_selector = pn.widgets.Select(name='Measure', options=measures, value=measures[0], width_policy="fit") 
    return precip_measure_selector


def get_flow_char_measure_selector() -> pn.widgets.Select:
    measures = [
        "sum",
        "max",
        "min",
        "mean"
    ]  
    flow_measure_selector = pn.widgets.Select(name='Measure', options=measures, value=measures[1], width_policy="fit") 
    return flow_measure_selector


def get_reference_time_player_all_dates() -> pn.widgets.Select:
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
) -> pn.widgets.DiscretePlayer:

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


def get_reference_time(reference_time) -> pn.pane.HTML:
    return pn.pane.HTML(f"Current Reference Time:   {reference_time}", 
                        sizing_mode = "stretch_width", 
                        style={'font-size': '15px', 'font-weight': 'bold'})


##################################### conversion stuff

def convert_area_to_ft2(
    units: str, 
    values: pd.Series
) -> pd.Series:
    
    if units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2) * (3.28**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values * (3.28**2)
    elif units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values
        
    return converted_values

def convert_area_to_mi2(
    units: str, 
    values: pd.Series
) -> pd.Series:
    
    if units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2) * (3.28**2) / (5280**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values * (3.28**2) / (5280**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (5280**2)
    elif units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values
        
    return converted_values
    
def convert_flow_to_cfs(
    units: str, 
    values: pd.Series
) -> pd.Series:
    
    if units in ['cms','m3/s']:
        converted_values = values * (3.28**3)
    elif units in ['cfs','ft3/s']:
        converted_values = values
        
    return converted_values 

def convert_area_to_m2(
    units: str, 
    values: pd.Series
) -> pd.Series:
    
    if units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2) / (3.28**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (3.28**2)
    elif units in ['km2','sqkm','km**2','km^2']:
        converted_values = values * (1000**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values
        
    return converted_values

def convert_area_to_km2(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    if units in ['mi2','sqmi','mi**2','mi^2']:
        converted_values = values * (5280**2) / (3.28**2) / (1000**2)
    elif units in ['ft2','sqft','ft**2','ft^2']:
        converted_values = values / (3.28**2) / (1000**2)
    elif units in ['m2','sqm','m**2','m^2']:
        converted_values = values / (1000**2)        
    elif units in ['km2','sqkm','km**2','km^2']:
        converted_values = values
        
    return converted_values
    
def convert_flow_to_cms(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    if units in ['cfs','ft3/s']:
        converted_values = values / (3.28**3)
    elif units in ['cms','m3/s']:
        converted_values = values
        
    return converted_values 

        
def convert_metrics_to_viz_units(
    gdf: gpd.GeoDataFrame, 
    viz_units: 'str',
) -> gpd.GeoDataFrame:
    
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
                    
    
def convert_attr_to_viz_units(
    df: pd.DataFrame, 
    viz_units: 'str',
) -> pd.DataFrame:
    
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


def combine_attributes(
    filelist: List[Path], 
    viz_units: str
) -> pd.DataFrame:
    
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