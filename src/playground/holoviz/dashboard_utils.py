import sys
sys.path.insert(0, '../../')
sys.path.insert(0, '../../evaluation/')

import teehr.queries.duckdb as tqd
import teehr.queries.utils as tqu
import duckdb as ddb
import temp_queries
import pandas as pd
import spatialpandas as spd
import panel as pn
import geopandas as gpd
import numpy as np
from datetime import datetime, timedelta
from evaluation import utils, config
from typing import List, Union
from pathlib import Path
import holoviews as hv
import geoviews as gv
import cartopy.crs as ccrs
import colorcet as cc
import datashader as ds
from bokeh.models import Range1d, LinearAxis

'''
misc utilities for TEEHR dashboards
''' 
    
def run_teehr_query(
    query_type: str,
    scenario: dict,
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
    include_geometry: Union[bool, None] = None,
) -> Union[str, pd.DataFrame, gpd.GeoDataFrame]:

    # start message - remove for now, figure out how to check if it was run by interaction
    #print(f"Executing TEEHR {query_type} query...")
    
    primary_filepath=scenario["primary_filepath"]
    secondary_filepath=scenario["secondary_filepath"]
    crosswalk_filepath=scenario["crosswalk_filepath"]
    
    if "geometry_filepath" in scenario.keys():
        geometry_filepath=scenario["geometry_filepath"]
    else:
        geometry_filepath=None
        
    if include_geometry is None:
        if geometry_filepath is None:
            include_geometry = False
        else:
            include_geometry = True
        
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
            include_geometry=include_geometry,
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
            include_geometry=include_geometry,
        )         
    
    # if type(gdf) in [gpd.GeoDataFrame, pd.DataFrame]:
    #     reftext = ""
    #     if "reference_time" in gdf.columns:
    #         reftext = f"and {gdf['reference_time'].nunique()} forecast reference times"
    #     print(f"TEEHR query complete for {gdf['primary_location_id'].nunique()} locations {reftext}")

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


def get_parquet_date_list_across_scenarios(
    pathlist: List[Path] = [],
    date_type: str = "value_time",
) -> List[pd.Timestamp]:
        
    print(f"Getting list of {date_type}s in the parquet files")   
    
    date_list = []
    for source in pathlist:

        if date_type == "reference_time":
            source_times = get_parquet_reference_time_list(source)
        elif date_type == "value_time":
            source_times = get_parquet_value_time_list(source)            
        else:
            print('add error catch')

        if source_times is not None:
            if len(date_list) == 0:
                date_list = source_times
            else:
                date_list = sorted(list(set(date_list).intersection(source_times)))
    return date_list


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
    if any(df.time.notnull()):
        return df.time.tolist()
    else:
        return None
    
def get_parquet_value_time_list(
    source: Path
) -> List[pd.Timestamp]:
    
    query = f"""select distinct(value_time)as time
        from '{source}'
        order by value_time asc"""
    df = ddb.query(query).to_df()
    if any(df.time.notnull()):
        return df.time.tolist()
    else:
        return None

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
    scenario_name: Union[str, None] = None,
    variable: Union[str, None] = None,
) -> Union[List[dict], dict]:
    
    scenario=[]
    for scenario_i in scenario_definitions:
        if scenario_name is not None and variable is not None:
            if scenario_i["scenario_name"] == scenario_name and scenario_i["variable"] == variable:
                scenario = scenario_i
        elif scenario_name is not None and variable is None:
            if scenario_i["scenario_name"] == scenario_name:
                scenario.append(scenario_i) 
        elif scenario_name is None and variable is not None:
            if scenario["variable"] == variable:
                scenario.append(scenario_i)                             
        
    return scenario


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

def get_reference_time_text(reference_time) -> pn.pane.HTML:
    return pn.pane.HTML(f"Current Reference Time:   {reference_time}", 
                        sizing_mode = "stretch_width", 
                        style={'font-size': '15px', 'font-weight': 'bold'})


def get_reference_time_player_all_dates(
    scenario: Union[dict, List[dict]] = {},
    opts: dict = {},
) -> pn.Column:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
        
    reference_times = get_parquet_date_list_across_scenarios(pathlist, date_type = "reference_time")
        
    reference_time_player = pn.widgets.DiscretePlayer(name='Discrete Player', 
                                                      options=list(reference_times), 
                                                      value=reference_times[0], 
                                                      interval=5000)   
    return reference_time_player


def get_reference_time_player_selected_dates(
    scenario: Union[dict, List[dict]] = {},
    start: pd.Timestamp = None,
    end: pd.Timestamp = None,
    opts: dict = {},
) -> pn.widgets.DiscretePlayer:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
        
    reference_times = get_parquet_date_list_across_scenarios(pathlist, date_type = "reference_time")

    reference_times_in_event = [t for t in reference_times if t >= start and t <= end]
    reference_time_player = pn.widgets.DiscretePlayer(name='Discrete Player', 
                                                      options=list(reference_times_in_event), 
                                                      value=reference_times_in_event[0], 
                                                      interval=5000,
                                                      show_loop_controls = False,
                                                      width_policy="fit",
                                                      margin=0,
                                                      **opts,
                                                      )   
    return reference_time_player



def get_reference_time_slider(
    scenario: Union[dict, List[dict]] = {},
    opts: dict = dict(width = 700, bar_color = "red", step=3600000*6),
) -> pn.Column:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
    
    reference_time_slider = get_date_range_slider_with_range_as_title(
        pathlist=pathlist,
        date_type='reference_time',
        opts = opts,
    )   
    return reference_time_slider


def get_value_time_slider(
    scenario: Union[dict, List[dict]] = {},
    opts: dict = dict(width = 700, bar_color = "green", step=3600000),
) -> pn.Column:
    
    if type(scenario) is list:
        pathlist = []
        for scenario_i in scenario:
            pathlist.extend([scenario_i["primary_filepath"], scenario_i["secondary_filepath"]])
    else:
        pathlist = [scenario["primary_filepath"], scenario["secondary_filepath"]]
    
    value_time_slider = get_date_range_slider_with_range_as_title(
        pathlist=pathlist,
        date_type='value_time', 
        opts = opts,
    )    
    return value_time_slider

def get_value_time_slider_selected_scenario_name(
    scenario_definitions: List[dict],
    scenario_name: str,
) -> pn.Column:
    
    scenario = get_scenario(scenario_definitions, scenario_name)
    value_time_slider = get_value_time_slider(scenario)
        
    return value_time_slider


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


def get_metric_selector(
    variable: str,
    metric_list: Union[List[str], None] = None,
) -> pn.widgets.MultiSelect:
    
    if metric_list is None:
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

def get_filter_widgets(
    scenario: dict = {},
) -> List:
    
    value_time_slider = get_value_time_slider(scenario)
    
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

def convert_depth_to_mm(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    if units in ['in','inches','in/hr']:
        converted_values = values * 25.4
    elif units in ['ft','feet','ft/hr']:
        converted_values = values * 12 * 25.4
    elif units in ['cm','cm/hr']:
        converted_values = values * 10
    elif units in ['m','m/hr']:
        converted_values = values * 1000
    elif units in ['mm','mm/hr']:
        converted_values = values        
    return converted_values 

def convert_depth_to_in(
    units: str, 
    values: pd.Series,
) -> pd.Series:
    
    if units in ['in','inches','in/hr']:
        converted_values = values
    elif units in ['ft','feet','ft/hr']:
        converted_values = values * 12
    elif units in ['cm','cm/hr']:
        converted_values = values / 2.54
    elif units in ['m','m/hr']:
        converted_values = values / .254
    elif units in ['mm','mm/hr']:
        converted_values = values / 25.4       
    return converted_values 

        
def convert_query_to_viz_units(
    gdf: Union[pd.DataFrame, gpd.GeoDataFrame], 
    viz_units: 'str',
    variable: Union['str', None] = None,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    
    # need a metric library to look up units and ranges
    
    convert_columns = [
        "bias",
        "secondary_average",
        "primary_average",
        "secondary_minimum",
        "primary_minimum",
        "primary_maximum",
        "secondary_maximum",
        "max_value_delta",
        "secondary_sum",
        "primary_sum",
        "secondary_variance",
        "primary_variance"]
    
    if 'variable_name' in gdf.columns:
        variable=gdf['variable_name'][0]
    
    measurement_unit = gdf['measurement_unit'][0]
    if variable in ['streamflow','flow','discharge']:
        if viz_units == 'english':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_flow_to_cfs(measurement_unit, gdf[col])
            gdf['measurement_unit'] = 'ft3/s'
        elif viz_units == 'metric':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_flow_to_cms(measurement_unit, gdf[col])  
            gdf['measurement_unit'] = 'm3/s'
            
    elif variable in ['precip','precipitation','precipitation_rate']:
        if viz_units == 'english':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_depth_to_in(measurement_unit, gdf[col])
            gdf['measurement_unit'] = 'in/hr'
        elif viz_units == 'metric':
            for col in gdf.columns:
                if col in convert_columns:
                    gdf[col] = convert_depth_to_mm(measurement_unit, gdf[col])  
            gdf['measurement_unit'] = 'mm/hr'        

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

def get_normalized_streamflow(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    area_col: str = 'upstream_area_value',
    include_metrics: Union[List[str], None] = None,
    units: str = 'metric',
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    
    if include_metrics is None:
        print('Metric list empty for normalization')
        return df
    else:
        flow_metrics = get_flow_metrics(include_metrics)
        if flow_metrics is not None:
            for metric in flow_metrics:
                norm_metric = '_'.join([metric, 'norm'])
                if units == 'metric':
                    df[norm_metric] = df[metric]/df[area_col]/(1000)*3600
                else:
                    df[norm_metric] = df[metric]/df[area_col]/(5280**2)*12*3600

            return df

def get_flow_metrics(metric_list: Union[List[str], None]) -> List[str]:

    flow_metrics = [
        "secondary_average",
        "primary_average",
        "secondary_minimum",
        "primary_minimum",            
        "primary_maximum",              
        "secondary_maximum",    
        "max_value_delta",                        
        "secondary_sum",
        "primary_sum",
        "secondary_variance",
        "primary_variance",
    ]
    if metric_list is not None:               
        return [metric for metric in metric_list if metric in flow_metrics]


####################### holoviews

def get_aggregator(measure):
    '''
    datashader aggregator function
    '''
    return ds.mean(measure)


def get_all_points(scenario: dict) -> hv.Points:

    df = pd.read_parquet(scenario["geometry_filepath"])
    gdf = tqu.df_to_gdf(df)
    gdf['easting'] = gdf.geometry.x
    gdf['northing'] = gdf.geometry.y
    
    return hv.Points(gdf, kdims = ['easting','northing'], vdims = ['id'])


def build_points_from_query(
    scenario: dict,
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
    group_by: Union[List[str], None] = None,
    order_by: Union[List[str], None] = None,       
    include_metrics: Union[List[str], None] = None,
    metric_limits: Union[dict, None] = None,  
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
) -> hv.Points:
    
    gdf = run_teehr_query(
        query_type="metrics",
        scenario=scenario,
        location_id=location_id,
        huc_id=huc_id,
        order_limit=order_limit,
        value_time_start=value_time_start,
        value_time_end=value_time_end,
        reference_time_single=reference_time_single,
        reference_time_start=reference_time_start,
        reference_time_end=reference_time_end,
        value_min=value_min,
        value_max=value_max,
        include_metrics=include_metrics,
        group_by=group_by,
        order_by=order_by,
        attribute_paths=attribute_paths
    )
    # convert units if necessary, add attributes, normalize
    gdf = convert_query_to_viz_units(gdf, units, scenario['variable'])
    attribute_df = combine_attributes(attribute_paths,units)
    gdf = merge_attr_to_gdf(gdf, attribute_df)
    gdf['max_perc_diff'] = gdf['max_value_delta']/gdf['primary_maximum']*100
    include_metrics = include_metrics + ['max_perc_diff','upstream_area_value','ecoregion_L2_value','stream_order_value']
    
    # gdf = get_normalized_streamflow(gdf, include_metrics=include_metrics)
    # norm_metrics = [col for col in gdf.columns if col[-4:]=='norm']
    if metric_limits is not None:
        for metric in gdf.columns:
            if metric in metric_limits.keys():
                limits = metric_limits[metric]
                gdf = gdf[(gdf[metric] >= limits[0]) & (gdf[metric] <= limits[1])]       
    
    # leave out geometry - easier to work with the data
    df = gdf.loc[:,[c for c in gdf.columns if c!='geometry']] 
    df['easting']=gdf.to_crs("EPSG:3857").geometry.x
    df['northing']=gdf.to_crs("EPSG:3857").geometry.y
    
    # for now limit vdims to peaks, % peak diff, and id
    #vdim_metrics = list(set(include_metrics + norm_metrics))
    vdims = [('max_perc_diff', 'peak % diff'),
             ('primary_maximum', 'observed_peak'),
             ('secondary_maximum', 'forecast_peak'),
             ('primary_location_id','id'),
             ('upstream_area_value','drainage_area')]
    kdims = ['easting','northing']    

    return hv.Points(df, kdims=kdims, vdims=vdims)


def build_polygons_from_query(
    scenario: dict,
    location_id: Union[str, List[str], None] = None,    
    huc_id: Union[str, List[str], None] = None,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None,
    group_by: Union[List[str], None] = None,
    order_by: Union[List[str], None] = None,       
    include_metrics: Union[List[str], None] = None,
    metric_limits: Union[dict, None] = None,  
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
) -> hv.Points:
    
    gdf = run_teehr_query(
        query_type="metrics",
        scenario=scenario,
        location_id=location_id,
        huc_id=huc_id,
        value_time_start=value_time_start,
        value_time_end=value_time_end,
        reference_time_single=reference_time_single,
        reference_time_start=reference_time_start,
        reference_time_end=reference_time_end,
        value_min=value_min,
        value_max=value_max,
        include_metrics=include_metrics,
        group_by=group_by,
        order_by=order_by,
        attribute_paths=attribute_paths
    )
    
    # convert units if necessary, add attributes, normalize
    gdf = gdf.to_crs("EPSG:3857")
    gdf = convert_query_to_viz_units(gdf, units, scenario['variable'])
    gdf['sum_diff'] = gdf['secondary_sum']-gdf['primary_sum']
    include_metrics = include_metrics + ['sum_diff']

    if metric_limits is not None:
        for metric in gdf.columns:
            if metric in metric_limits.keys():
                limits = metric_limits[metric]
                gdf = gdf[(gdf[metric] >= limits[0]) & (gdf[metric] <= limits[1])]       
    
    #convert to spatialpandas object
    sdf = spd.GeoDataFrame(gdf)
    
    #vdim_metrics = list(set(include_metrics + norm_metrics))
    vdims = include_metrics + [('primary_location_id','id')] 

    return gv.Polygons(sdf, crs=ccrs.GOOGLE_MERCATOR, vdims=vdims)


def build_hyetograph_from_query_selected_point(
    index: List[int],
    points_dmap: hv.DynamicMap,
    scenario: dict,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None, 
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
    opts = {},
) -> hv.Points:
    
    if len(index) > 0 and len(points_dmap.dimensions('value')) > 0:  
        point_id = points_dmap.dimension_values('primary_location_id')[index][0]
        cross = pd.read_parquet(attribute_paths['usgs_huc_crosswalk'])
        huc12_id = cross.loc[cross['primary_location_id']==point_id, 'secondary_location_id'].iloc[0]
        huc10_id = "-".join(['huc10', huc12_id.split("-")[1][:10]])
        title = f"{huc10_id} (Contains Gage: {point_id})"     
        
        df = run_teehr_query(
            query_type="timeseries",
            scenario=scenario,
            location_id=huc10_id,
            value_time_start=value_time_start,
            value_time_end=value_time_end,
            reference_time_single=reference_time_single,
            reference_time_start=reference_time_start,
            reference_time_end=reference_time_end,
            value_min=value_min,
            value_max=value_max,
            order_by=['primary_location_id','reference_time','value_time'],
            attribute_paths=attribute_paths,
            include_geometry=False,
        )            
        df = convert_query_to_viz_units(df, units, 'precipitation')
        
        df['value_time_str'] = df['value_time'].dt.strftime('%Y-%m-%d-%H')
        time_start = df['value_time'].min()
        time_end = df['value_time'].max()
        t = time_start + (time_end - time_start)*0.01
        text_x = t.replace(second=0, microsecond=0, minute=0).strftime('%Y-%m-%d-%H')

        if units == 'metric':
            unit_rate_label = 'mm/hr'
            unit_cum_label = 'mm'
        else:
            unit_rate_label = 'in/hr'
            unit_cum_label = 'in'
        
        if 'value' in df.columns:  #single timeseries
            df['cumulative'] = df['value'].cumsum()

            data_max = df['value'].max()
            ymax_bars = max(data_max*1.1,1)
            ymax_curve = max(data_max*1.1,1)
            text_y = ymax_bars*0.9   
            text_label = hv.Text(text_x, text_y, title).opts(text_align='left', text_font_size='10pt', 
                            text_color='#57504d', text_font_style='bold')               

            bars = hv.Bars(df, kdims = [('value_time_str','Date')], vdims = [('value', 'Precip Rate ' + unit_rate_label)])
            curve = hv.Curve(df, kdims = [('value_time_str', 'Date')], vdims = [('cum', 'Precip ' + unit_cum_label)])

            bars.opts(**opts, fill_color = 'blue', line_color = None, ylim=(0, ymax_bars))
            curve.opts(**opts, color='orange', hooks=[plot_secondary_bars_curve])
            ts_layout_hv = (bars * curve * text_label).opts(show_title=False)  

        else:

            df['primary_cumulative'] = df['primary_value'].cumsum()
            df['secondary_cumulative'] = df['secondary_value'].cumsum()
            data_max = max(df['primary_value'].max(), df['secondary_value'].max())
            data_max_cum = max(df['primary_cumulative'].max(), df['secondary_cumulative'].max())
            
            ###  need to fix ymax so both cumulative 
            
            ymax_bars = max(data_max*1.1,1)
            ymax_curve = max(data_max_cum*1.1,1)
            text_y = ymax_bars*0.9   
            text_label = hv.Text(text_x, text_y, title).opts(text_align='left', text_font_size='10pt', 
                            text_color='#57504d', text_font_style='bold')  
            
            bars_prim = hv.Bars(df, kdims = [('value_time_str','Date')], 
                                vdims = [('primary_value', 'Precip Rate ' + unit_rate_label)])          
            curve_prim = hv.Curve(df, kdims = [('value_time_str', 'Date')], 
                                  vdims = [('primary_cumulative', 'Precip ' + unit_cum_label)])
            
            bars_sec = hv.Bars(df, kdims = [('value_time_str','Date')], 
                               vdims = [('secondary_value', 'Precip Rate ' + unit_rate_label)])
            curve_sec = hv.Curve(df, kdims = [('value_time_str', 'Date')], 
                                 vdims = [('secondary_cumulative', 'Precip ' + unit_cum_label)])

            bars_prim.opts(**opts, fill_color = 'dodgerblue', line_color = None, ylim=(0, ymax_bars))
            curve_prim.opts(**opts, color='blue', ylim=(0, ymax_curve), hooks=[plot_secondary_bars_curve])    
            
            bars_sec.opts(**opts, fill_color = 'sandybrown', line_color = None, ylim=(0, ymax_bars))
            curve_sec.opts(**opts, color='orange', ylim=(0, ymax_curve), hooks=[plot_secondary_bars_curve])    
            
            ts_layout_hv = (bars_prim * bars_sec * curve_prim * curve_sec * text_label).opts(show_title=False)  
     
    else:        
        df = pd.DataFrame([[0,0],[1,0]], columns = ['Date','value'])
        label = "Nothing Selected"
        curve = hv.Curve(df, "Date", "value").opts(**opts)
        text_label = hv.Text(0.01, 0.9, "No Selection").opts(text_align='left', text_font_size='10pt', 
                                                       text_color='#57504d', text_font_style='bold')
        ts_layout_hv = (curve * text_label).opts(show_title=False)
            
    return ts_layout_hv  


def build_hydrograph_from_query_selected_point(
    index: List[int],
    points_dmap: hv.DynamicMap,
    scenario: dict,
    value_time_start: Union[pd.Timestamp, None] = None,
    value_time_end: Union[pd.Timestamp, None] = None,    
    reference_time_single: Union[pd.Timestamp, None] = None,
    reference_time_start: Union[pd.Timestamp, None] = None,
    reference_time_end: Union[pd.Timestamp, None] = None,
    value_min: Union[float, None] = None,
    value_max: Union[float, None] = None, 
    attribute_paths: Union[List[Path], None] = None,
    units: str = "metric",
    opts = {},
) -> hv.Points:
    
    if len(index) > 0 and len(points_dmap.dimensions('value')) > 0:  
        point_id = points_dmap.dimension_values('primary_location_id')[index][0]
        area = points_dmap.dimension_values('upstream_area_value')[index][0]
        title = f"Gage ID: {point_id}"
        
        df = run_teehr_query(
            query_type="timeseries",
            scenario=scenario,
            location_id=point_id,
            value_time_start=value_time_start,
            value_time_end=value_time_end,
            reference_time_single=reference_time_single,
            reference_time_start=reference_time_start,
            reference_time_end=reference_time_end,
            value_min=value_min,
            value_max=value_max,
            order_by=['primary_location_id','reference_time','value_time'],
            attribute_paths=attribute_paths,
            include_geometry=False,
        )      
        df = convert_query_to_viz_units(df, units, 'streamflow')      
        
        time_start = df['value_time'].min()
        time_end = df['value_time'].max()
        t = time_start + (time_end - time_start)*0.01
        text_x = t.replace(second=0, microsecond=0, minute=0).strftime('%Y-%m-%d-%H')

        if units == 'metric':
            unit_label = 'cms'
            unit_norm_label = 'mm/hr'
            conversion = 3600*1000/1000**2
        else:
            unit_label = 'cfs'
            unit_norm_label = 'in/hr'
            conversion = 3600*12/5280**2
        
        if 'value' in df.columns:  #single timeseries
            df['normalized'] = df['value']/area*conversion

            data_max = df['value'].max()
            data_max_norm = df['normalized'].max()
            ymax_flow = max(data_max*1.1,1)
            ymax_norm = max(data_max_norm*1.1,1)
            text_y = ymax_flow*0.9   
            text_label = hv.Text(text_x, text_y, title).opts(text_align='left', text_font_size='10pt', 
                                           text_color='#57504d', text_font_style='bold')  

            flow = hv.Curve(df, kdims = [('value_time', 'Date')], vdims = [('value', 'Flow ' + unit_label)])
            norm = hv.Curve(df, kdims = [('value_time', 'Date')], vdims = [('normalized', 'Normalized Flow ' + unit_norm_label)])

            flow.opts(**opts, color='blue', ylim=(0, ymax_flow))
            norm.opts(**opts, color='orange', alpha=0, ylim=(0, ymax_norm), hooks=[plot_secondary_bars_curve])

        else:

            df['primary_normalized'] = df['primary_value']/area*conversion
            df['secondary_normalized'] = df['secondary_value']/area*conversion
            prim_max = df['primary_value'].max()
            sec_max = df['secondary_value'].max()
            data_max = max(prim_max, sec_max)
            data_max_norm = max(df['primary_normalized'].max(), df['secondary_normalized'].max())
            
            ymax_flow = max(data_max*1.1,1)
            ymax_norm = max(data_max_norm*1.1,1)
            text_y = ymax_flow*0.9  
            text_label = hv.Text(text_x, text_y, title).opts(text_align='left', text_font_size='10pt', 
                                           text_color='#57504d', text_font_style='bold')  

            flow_prim = hv.Curve(df, kdims = [('value_time', 'Date')], 
                                 vdims = [('primary_value', 'Flow ' + unit_label)])
            norm_prim = hv.Curve(df, kdims = [('value_time', 'Date')], 
                                 vdims = [('primary_normalized', 'Normalized Flow ' + unit_norm_label)])
            
            flow_sec = hv.Curve(df, kdims = [('value_time', 'Date')], 
                                vdims = [('secondary_value', 'Flow ' + unit_label)])
            norm_sec = hv.Curve(df, kdims = [('value_time', 'Date')], 
                                vdims = [('secondary_normalized', 'Normalized Flow ' + unit_norm_label)])

            flow_prim.opts(**opts, color='dodgerblue', ylim=(0, ymax_flow))
            norm_prim.opts(**opts, color='blue', alpha=0, ylim=(0, ymax_norm), hooks=[plot_secondary_curve_curve])  
            
            flow_sec.opts(**opts, color='sandybrown', ylim=(0, ymax_flow))
            norm_sec.opts(**opts, color='orange', alpha=0, ylim=(0, ymax_norm), hooks=[plot_secondary_curve_curve]) 
          
            if prim_max > sec_max:
                ts_layout_hv = (flow_prim * flow_sec * norm_prim * norm_sec * text_label).opts(show_title=False)  
            else:
                ts_layout_hv = (flow_sec * flow_prim * norm_sec * norm_prim * text_label).opts(show_title=False)

    else:        
        df = pd.DataFrame([[0,0],[1,0]], columns = ['Date','value'])
        label = "Nothing Selected"
        curve = hv.Curve(df, "Date", "value").opts(**opts)
        text = hv.Text(0.01, 0.9, "No Selection").opts(text_align='left', text_font_size='10pt', 
                                                       text_color='#57504d', text_font_style='bold')
        ts_layout_hv = (curve * text).opts(show_title=False)
            
    return ts_layout_hv  



#################################### 

def get_precip_colormap():
    ''' 
    build custom precip colormap 
    '''
    cmap1 = cc.CET_L6[85:]
    cmap2 = [cmap1[i] for i in range(0, len(cmap1), 3)]
    ext = [cmap2[-1] + aa for aa in ['00','10','30','60','99']]
    cmap = ext + cmap2[::-1] + cc.CET_R1
    
    return cmap

def get_recurr_colormap():
    ''' 
    build explicit colormap for 2, 5, 10, 25, 50, 100 recurrence intervals
    based on OWP High Flow Magnitude product
    '''    
    cmap = {0: 'lightgray', 
            2: 'dodgerblue', 
            5: 'yellow', 
            10: 'darkorange', 
            25: 'red', 
            50: 'fuchsia', 
            100: 'darkviolet'}
    
    return cmap


def plot_secondary_bars_curve(plot, element):
    """
    Hook to plot data on a secondary (twin) axis on a Holoviews Plot with Bokeh backend.
    More info:
    - http://holoviews.org/user_guide/Customizing_Plots.html#plot-hooks
    - https://docs.bokeh.org/en/latest/docs/user_guide/plotting.html#twin-axes
    """
    fig: Figure = plot.state
    glyph_first: GlyphRenderer = fig.renderers[0]  # will be the original plot
    glyph_last: GlyphRenderer = fig.renderers[-1] # will be the new plot
    right_axis_name = "twiny"
    # Create both axes if right axis does not exist
    if right_axis_name not in fig.extra_y_ranges.keys():
        # Recreate primary axis (left)
        y_first_name = glyph_first.glyph.top
        y_first_min = glyph_first.data_source.data[y_first_name].min()
        y_first_max = glyph_first.data_source.data[y_first_name].max()
        y_first_offset = (y_first_max - y_first_min) * 0.1
        fig.y_range = Range1d(
            start=0,
            end=max(y_first_max,1) + y_first_offset
       )
        fig.y_range.name = glyph_first.y_range_name
        # Create secondary axis (right)
        y_last_name = glyph_last.glyph.y
        y_last_min = glyph_last.data_source.data[y_last_name].min()
        y_last_max = glyph_last.data_source.data[y_last_name].max()
        y_last_offset = (y_last_max - y_last_min) * 0.1
        fig.extra_y_ranges = {right_axis_name: Range1d(
            start=0,
            end=max(y_last_max,1) + y_last_offset
        )}
        fig.add_layout(LinearAxis(y_range_name=right_axis_name, axis_label=glyph_last.glyph.y), "right")
    # Set right axis for the last glyph added to the figure
    glyph_last.y_range_name = right_axis_name
    
    
def plot_secondary_curve_curve(plot, element):
    """
    Hook to plot data on a secondary (twin) axis on a Holoviews Plot with Bokeh backend.
    More info:
    - http://holoviews.org/user_guide/Customizing_Plots.html#plot-hooks
    - https://docs.bokeh.org/en/latest/docs/user_guide/plotting.html#twin-axes
    """
    fig: Figure = plot.state
    glyph_first: GlyphRenderer = fig.renderers[0]  # will be the original plot
    glyph_last: GlyphRenderer = fig.renderers[-1] # will be the new plot
    right_axis_name = "twiny"
    # Create both axes if right axis does not exist
    if right_axis_name not in fig.extra_y_ranges.keys():
        # Recreate primary axis (left)
        y_first_name = glyph_first.glyph.y
        y_first_min = glyph_first.data_source.data[y_first_name].min()
        y_first_max = glyph_first.data_source.data[y_first_name].max()
        y_first_offset = (y_first_max) * 0.1
        fig.y_range = Range1d(
            start=0,
            end=y_first_max + y_first_offset
       )
        # Create secondary axis (right)
        y_last_name = glyph_last.glyph.y
        y_last_min = glyph_last.data_source.data[y_last_name].min()
        y_last_max = glyph_last.data_source.data[y_last_name].max()
        y_last_offset = (y_last_max) * 0.1
        fig.extra_y_ranges = {right_axis_name: Range1d(
            start=0,
            end=y_last_max + y_last_offset
        )}
        fig.add_layout(LinearAxis(y_range_name=right_axis_name, axis_label=glyph_last.glyph.y), "right")
    # Set right axis for the last glyph added to the figure
    glyph_last.y_range_name = right_axis_name
