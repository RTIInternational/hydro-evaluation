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
    
def run_teehr_query(
    query_type: str,
    primary_filepath: Path,
    secondary_filepath: Path,
    crosswalk_filepath: Path,
    geometry_filepath: Union[Path, None] = None,
    location_id: Union[str, List[str], None] = None,    
    huc_id: Union[str, List[str], None] = None,
    order_limit: Union[int, None] = None,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None,
    include_metrics: Union[List[str], None] = None,
    group_by: Union[List[str], None] = None,
    order_by: Union[List[str], None] = None,
    attribute_paths: Union[List[Path], None] = None,
    return_query: Union[bool, None] = False,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:

    # start message
    print(f"Executing TEEHR {query_type} query...")
    
    # set the geometry flag
    geom_flag = True
    if geometry_filepath is None:
        geom_flag = False
        
    # initialize group_by and order_by lists
    if group_by is None:
        group_by=["primary_location_id","measurement_unit"]
    else:
        group_by.extend(["primary_location_id","measurement_unit"])
        group_by = list(set(group_by))
    if order_by is None:
        order_by=["primary_location_id"]

    filters=[]
    # build the filters
    
    # region (HUC) id
    if huc_id is not None:
        if huc_id != 'all':
            huc_level = len(huc_id)

            # if usgs, get the crosswalk (for now huc level must be 10 or smaller)
            if primary_filepath.parent.name == 'usgs':
                location_list = get_usgs_locations_within_huc(huc_level, huc_id, attribute_paths)
                filters.append(
                    {
                        "column": "primary_location_id",
                        "operator": "in",
                        "value": location_list
                    }
                )
            elif primary_filepath.parent.name in ['analysis_assim','analysis_assim_extend']:
                location_list = get_nwm_locations_within_huc(huc_level, huc_id, attribute_paths)
                filters.append(
                    {
                        "column": "primary_location_id",
                        "operator": "in",
                        "value": location_list
                    } 
                )
            elif primary_filepath.parent.name in ['forcing_analysis_assim','forcing_analysis_assim_extend']:
                    filters.append(
                        {
                            "column": "primary_location_id",
                            "operator": "like",
                            "value": f"huc10-{huc_id}%"
                        }
                    )

    # location id (not using in current notebook example, using huc_id instead, leaving in for future use)
    if location_id is not None:    
        if type(location_id) is str:
            filters.append(
                {
                    "column": "primary_location_id",
                    "operator": "like",
                    "value": f"{location_id}%"
                }
            )
        elif type(location_id) is list:
            filters.append(
                {
                    "column": "primary_location_id",
                    "operator": "in",
                    "value": location_id
                }
            )       
            
    # stream order
    if order_limit is not None:
        if type(order_limit) is int:
            location_list = get_usgs_locations_below_order_limit(order_limit, attribute_paths)
            filters.append(
                {
                    "column": "primary_location_id",
                    "operator": "in",
                    "value": location_list
                }  
            )
         
    # reference times
    if reference_time_single is not None:
        filters.append(
            {
                "column": "reference_time",
                "operator": "=",
                "value": f"{reference_time_single}"
            }
        )
    elif reference_time_start is not None:
        filters.append(
            {
                "column": "reference_time",
                "operator": ">=",
                "value": f"{reference_time_start}"
            }
        )
        if reference_time_end is not None:
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
    if value_time_start is not None:
        filters.append(
            {
                "column": "value_time",
                "operator": ">=",
                "value": f"{value_time_start}"
            }
        )
        if value_time_end is not None:
            filters.append(
                {
                    "column": "value_time",
                    "operator": "<=",
                    "value": f"{value_time_end}"
                } 
            )
        else:             
            print('set up an error catch here')        
                        
                        
        
    # min/max values --  ***  will eventually need to allow requiring one or other (primary or secondary) to be above threshold
    #                         for recurrence flows, threshold is different at every location, future work to add this to queries 
    if value_min is not None:  
        if type(value_min) is str:
            value_min = float(value_min.split(' ')[0])       
        filters.extend(
            [{
                "column": "primary_value",
                "operator": ">=",
                "value": value_min
            },
            {
                "column": "secondary_value",
                "operator": ">=",
                "value": value_min
            }]
        )
            
    if value_max is not None:
        if type(value_min) is str:
            value_min = float(value_min.split(' ')[0])  
        filters.extend(
            [{
                "column": "primary_value",
                "operator": "<=",
                "value": value_max
            },
            {
                "column": "secondary_value",
                "operator": "<=",
                "value": value_max
            }]
        )
        
    if query_type == "metrics":

        if include_metrics is None:
            include_metrics = "all"

        # get metrics
        gdf = tqd.get_metrics(
            primary_filepath,
            secondary_filepath,
            crosswalk_filepath,        
            group_by=group_by,
            order_by=order_by,
            filters=filters,
            return_query=return_query,
            geometry_filepath=geometry_filepath,       
            include_geometry=geom_flag,
            include_metrics=include_metrics,
        )    
        
    else:
        # get timeseries
        gdf = tqd.get_joined_timeseries(
            primary_filepath,
            secondary_filepath,
            crosswalk_filepath,
            order_by=order_by,
            filters=filters,
            return_query=return_query,
            geometry_filepath=geometry_filepath,       
            include_geometry=geom_flag,
        )         
    
    if type(gdf) in [gpd.GeoDataFrame, pd.DataFrame]:
        print(f"TEEHR query complete for {gdf['primary_location_id'].nunique()} locations")
        # and {gdf['reference_time'].nunique()} forecast reference times")
        
    return gdf


################################## get info about the parquet files


def get_parquet_date_range_across_scenarios(
    pathlist: List[Path] = [],
    date_type: str = "value_time",
) -> List[pd.Timestamp]:
        
        
    print(f"Checking {date_type} range in the parquet files")    
      
    for source in pathlist:
        
        if date_type == "reference_time":
            [source_start_date, source_end_date] = get_parquet_reference_time_range(source)
        elif date_type == "value_time":
            [source_start_date, source_end_date] = get_parquet_value_time_range(source)            
        # elif date_type == "lead_time":
        #     [source_start_date, source_end_date] = get_parquet_lead_time_range(source) 
        else:
            print('add error catch')
            
        if source == pathlist[0]:
            start_date = source_start_date
            end_date = source_end_date
        else:
            if type(source_start_date) is pd.Timestamp and type(start_date) is pd.Timestamp:
                if source_start_date > start_date:
                    start_date = source_start_date
            elif type(source_start_date) is pd.Timestamp and type(start_date) != pd.Timestamp:
                start_date = source_start_date
                
            if type(source_end_date) is pd.Timestamp and type(end_date) is pd.Timestamp:                    
                if source_end_date < end_date:
                    end_date = source_end_date
            elif type(source_end_date) is pd.Timestamp and type(end_date) is not pd.Timestamp:
                end_date = source_end_date
                
    return [start_date, end_date]


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


def get_parquet_reference_time_list(
    source: Path
) -> List[pd.Timestamp]:
    
    query = f"""select distinct(reference_time)as time
        from '{source}'
        order by reference_time asc"""
    df = ddb.query(query).to_df()
    times = df.time.tolist()
    return times
                              

########### get info from inputs - location subset and scenario stuff


def get_usgs_locations_within_huc(
    huc_level: int = 2,
    huc_id: str = '01', 
    attribute_paths: dict = {},
) -> List[str]:
    
    location_list=[]
    if "usgs_huc_crosswalk" in attribute_paths.keys():
        df = pd.read_parquet(attribute_paths["usgs_huc_crosswalk"])
        df['huc'] = df['secondary_location_id'].str[6:6+huc_level]
        location_list = df[df['huc']==huc_id]['primary_location_id'].to_list()
        if not location_list:
           print(f"Warning, no usgs locations found in {huc_id}")     
    else:
        print("Warning, no usgs-huc crosswalk found, location list empty")
        
    return location_list


def get_nwm_locations_within_huc(
    huc_level: int = 2,
    huc_id: str = '01', 
    attribute_paths: dict = {},
    version: float = 2.2,
) -> List[str]:
    
    nwm_version = 'nwm' + str(version).replace('.','')
    crosswalk_name = "_".join([nwm_version, 'huc','crosswalk'])
    
    location_list=[]
    if crosswalk_name in attribute_paths.keys():
        df = pd.read_parquet(attribute_paths[crosswalk_name])
        df['huc'] = df['secondary_location_id'].str[6:6+huc_level]
        location_list = df[df['huc']==huc_id]['primary_location_id'].to_list()
        if not location_list:
           print(f"Warning, no nwm reaches locations found in {huc_id}")     
    else:
        print(f"Warning, no {crosswalk_name} found, location list empty")
        
    return location_list

def get_usgs_locations_below_order_limit(
    order_limit: int = 10,
    attribute_paths: dict = {},
) -> List[str]:
    
    location_list=[]
    if "usgs_stream_order" in attribute_paths.keys():
        df = pd.read_parquet(attribute_paths["usgs_stream_order"])
        location_list = df[df['attribute_value']<=order_limit]['location_id'].to_list()
        if not location_list:
           print(f"Warning, no usgs locations found with stream order <= {order_limit}")     
    else:
        print("Warning, no usgs stream order attribute table found, location list empty")
        
        
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


def get_scenario_names(scenario_definitions: List[dict]) -> List[str]:
    
    scenario_name_list = []
    for scenario in scenario_definitions:
        scenario_name_list.append(scenario["scenario_name"])
    
    return list(set(scenario_name_list))


def get_scenario_variables(scenario_definitions: List[dict]) -> List[str]:
    
    variable_list = []
    for scenario in scenario_definitions:
        variable_list.append(scenario["variable"])
    
    return list(set(variable_list))


def get_parquet_pathlist_from_scenario(
    scenario_definitions: List[dict],
    scenario_name: str,
    variable: str,
) -> List[Path]:
    
    pathlist=[]
    for scenario in scenario_definitions:
        if scenario["scenario_name"] == scenario_name and scenario["variable"] == variable:
            pathlist.append(scenario["primary_filepath"])
            pathlist.append(scenario["secondary_filepath"])

    return list(set(pathlist))


################################################# widgets

def get_filter_date_widgets(
    scenario: Union[dict, List[dict]] = {},
) -> List:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
    
    value_time_slider = get_date_range_slider_with_range_as_title(
        pathlist=pathlist,
        date_type='value_time', 
        opts = dict(width = 700, bar_color = "green", step=3600000)
    )
    reference_time_slider = get_date_range_slider_with_range_as_title(
        pathlist=pathlist,
        date_type='reference_time',
        opts = dict(width = 700, bar_color = "red", step=3600000*6)
    )   
    
    return [value_time_slider, reference_time_slider]

def get_filter_widgets(
    scenario: dict = {},
) -> List:
    
    value_time_slider = get_date_range_slider_with_range_as_title(
        pathlist=[scenario["primary_filepath"], scenario["secondary_filepath"]],
        date_type='value_time', 
        opts = dict(width = 700, bar_color = "green", step=3600000)
    )
    reference_time_slider = get_date_range_slider_with_range_as_title(
        pathlist=[scenario["primary_filepath"], scenario["secondary_filepath"]],
        date_type='reference_time',
        opts = dict(width = 700, bar_color = "red", step=3600000*6)
    )

    huc2_selector = get_huc2_selector()
    order_limit_selector = get_order_limit_selector()
    threshold_selector = get_threshold_selector(scenario['variable'])
    metric_selector = get_metric_selector(scenario['variable'])    
    
    return [value_time_slider, reference_time_slider, huc2_selector, 
            threshold_selector, order_limit_selector, metric_selector]

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


def get_minmax_dates_text(
    min_date: pd.Timestamp, 
    max_date: pd.Timestamp,
    date_type: str,
    opts: dict = {},
) -> pn.pane:
    return pn.pane.HTML(f"Range of {date_type} available in cache: {min_date} - {max_date}", 
                        sizing_mode = "stretch_width",
                        style={'font-size': '15px', 'font-weight': 'bold'})

def get_lead_time_selector() -> pn.widgets.Select:

    lead_time_options = ['all','1 day','3 day','5 day','10 day']
    lead_time_selector = pn.widgets.Select(name="Forecast lead times (coming soon)", 
                                           options=lead_time_options, value="all", width_policy="fit")

    return lead_time_selector


def get_huc2_selector() -> pn.widgets.Select:
    '''
    HUC2 region to explore, enables smaller region for faster responsiveness
    '''
    huc2_list = ["all"] + [str(huc2).zfill(2) for huc2 in list(range(1,19))]
    huc2_selector = pn.widgets.Select(name='HUC-2 Subregion', options=huc2_list, value="18", width_policy="fit")
    
    return huc2_selector       


def get_variable_selector(variable_list: List[str]) -> pn.widgets.Select:
    
    if 'streamflow' in variable_list:
        value='streamflow'
    else:
        value=variable_list[0]
    
    variable_selector = pn.widgets.Select(name='Evaluation variable', 
                                          options=variable_list, 
                                          value=value, 
                                          width_policy="fit")
    
    return variable_selector


def get_scenario_selector(scenario_name_list: List[str]) -> pn.widgets.Select:
    
    scenario_selector = pn.widgets.Select(name='Evaluation scenario', 
                                          options=scenario_name_list, 
                                          value=scenario_name_list[0], 
                                          width_policy="fit")
    
    return scenario_selector


def get_metric_selector(variable: str) -> pn.widgets.MultiSelect:
    
    if variable == 'streamflow':
        metric_list = [
        "bias",
        "nash_sutcliffe_efficiency",
        "kling_gupta_efficiency",
        "mean_squared_error",
        "root_mean_squared_error",   
        "secondary_count",
        "primary_count",
        "secondary_average",
        "primary_average",
        "secondary_minimum",
        "primary_minimum",            
        "primary_maximum",              
        "secondary_maximum",    
        "max_value_delta",            
        "primary_max_value_time",
        "secondary_max_value_time",
        "max_value_timedelta",             
        ]
        
    elif variable == 'precipitation':
        metric_list = [
        "bias",
        "secondary_sum",
        "primary_sum",
        "secondary_average",
        "primary_average",
        "secondary_variance",
        "primary_variance",        
        ]        
    
    metric_selector = pn.widgets.MultiSelect(name='Evaluation metrics', 
                                          options=metric_list, 
                                          value=[metric_list[0]], 
                                          width_policy="fit")
    
    return metric_selector


def get_order_limit_selector() -> pn.widgets.Select:
    
    order_list = ['None'] + list(range(1,11))
    order_limit_selector = pn.widgets.Select(name='Stream order upper limit', 
                                          options=order_list, 
                                          value=order_list[0], 
                                          width_policy="fit")
    
    return order_limit_selector

def get_threshold_selector(variable: str = 'streamflow') -> pn.widgets.Select:
    
    if variable == 'streamflow':
        threshold_list = [0, '0.1 cms', '1 cms', '2-year (coming soon)','10-year (coming soon)','100-year (coming soon)']
    elif variable == 'precipitation':
        threshold_list = ['0 mm/hr']
        
    threshold_selector = pn.widgets.Select(name='Value threshold (lower limit)', 
                                          options=threshold_list, 
                                          value=threshold_list[0], 
                                          width_policy="fit")
    
    return threshold_selector


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


def get_reference_time_text(reference_time) -> pn.pane.HTML:
    return pn.pane.HTML(f"Current Reference Time:   {reference_time}", 
                        sizing_mode = "stretch_width", 
                        style={'font-size': '15px', 'font-weight': 'bold'})



##############################################  attributes

def combine_attributes(
    attribute_paths: dict[Path], 
    viz_units: str
) -> pd.DataFrame:
    
    # note that any locations NOT included in all selected attribute tables will be excluded
    attr_df=pd.DataFrame()
    for key in attribute_paths:
        df = pd.read_parquet(attribute_paths[key])
        if 'attribute_name' in df.columns:
            attribute_name = df['attribute_name'].iloc[0]
            attribute_unit = df['attribute_unit'].iloc[0]
            if attribute_unit != 'none':
                df = convert_attr_to_viz_units(df, viz_units)      
                df = df.rename(columns = {'attribute_value': attribute_name + '_value', 
                                              'attribute_unit': attribute_name + '_unit'})
            else:
                df = df.drop('attribute_unit', axis=1)   
                df = df.rename(columns = {'attribute_value': attribute_name + '_value'})
                
            df = df.drop('attribute_name', axis=1)
            if attr_df.empty:
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


##################################### unit conversion stuff

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
    
    attr_units = df['attribute_unit'][0]
    attr_name = df['attribute_name'][0]
    
    if viz_units == 'english':
        if attr_name.find('area')>=0:
            df['attribute_value'] = convert_area_to_mi2(attr_units, df['attribute_value'])
            df['attribute_unit'] = 'mi2'
        elif attr_name.find('flow')>=0:
            df['attribute_value'] = convert_area_to_cfs(attr_units, df['attribute_value'])
            df['attribute_unit'] = 'cfs'   
            
    elif viz_units == 'metric':
        if attr_name.find('area')>=0:
            df['attribute_value'] = convert_area_to_km2(attr_units, df['attribute_value'])
            df['attribute_unit'] = 'km2'
        elif attr_name.find('flow')>=0:
            df['attribute_value'] = convert_area_to_cms(attr_units, df['attribute_value'])
            df['attribute_unit'] = 'cms'   
        
    return df

