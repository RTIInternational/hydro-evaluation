import sys
sys.path.insert(0, '../../')
sys.path.insert(0, '../../evaluation/')

import duckdb as ddb
import temp_queries as queries
import pandas as pd
import geopandas as gpd
import numpy as np
import panel as pn
from datetime import datetime, timedelta
import colorcet as cc
from bokeh.models import Range1d, LinearAxis

from evaluation import utils, config
import temp_queries

from shapely.geometry import Point
import cartopy.crs as ccrs

'''
functions used in dashboard 1 
'''

###### read data

def read_points(streamflow_info):
    points_gdf = utils.get_usgs_gages()
    points_gdf = points_gdf.to_crs("EPSG:3857")
    points_gdf = points_gdf[[streamflow_info['geom_location_id_header'],
                             'nwm_feature_id',
                             'latitude',
                             'longitude',
                             'geometry',
                            ]]    
    # add easting and northing - allows simple plotting as points on basemap
    points_gdf['easting'] = points_gdf.geometry.x
    points_gdf['northing'] = points_gdf.geometry.y      
    return points_gdf


def query_historical_data(   
    data_info: dict,
    data_location_id_like_string: str = "all", 
    start_value_time: pd.Timestamp = None,
    end_value_time: pd.Timestamp = None,
    data_type: str = "timeseries",          # 'timeseries' or 'chars'       
) -> pd.DataFrame:
    '''
    Run DuckDB query to extract historical timeseries data 
    or time series characteristics 
    by region (portion of ID) and value_time range
    '''   
    data_source = data_info['source']
    data_location_id_header = data_info['data_location_id_header']
    
    # build filters
    filters = temp_queries.get_historical_filters(
        data_source, 
        data_location_id_header, 
        data_location_id_like_string, 
        start_value_time, 
        end_value_time
    )
    #build query
    if data_type == 'timeseries':
        query = temp_queries.get_historical_timeseries_data_query(
            data_source, 
            data_location_id_header,
            filters=filters
        )
    elif data_type == 'chars':
        query = temp_queries.get_historical_timeseries_chars_query(
            data_source, 
            group_by=[data_location_id_header],
            order_by=[data_location_id_header],
            filters=filters
        )        
    #run query
    df = ddb.query(query).to_df()

    return df

def get_historical_chars_with_geom(
    data_info: dict,    
    data_location_id_like_string: str, 
    start_value_time: pd.Timestamp = None,
    end_value_time: pd.Timestamp = None,
    variable_name = None,      
    geom_gdf: gpd.GeoDataFrame() = None,      
) -> gpd.GeoDataFrame:
    '''
    query data chars
    merge with geometry
    convert units or add recurr flows, if streamflow
    '''
    data_df = query_historical_data(
        data_info,
        data_location_id_like_string = data_location_id_like_string,
        start_value_time = start_value_time, 
        end_value_time = end_value_time,
        data_type = "chars"
    )          
    # merge with geodataframe (must do this before adding recurrence flows so have the nwm_feature_id)
    data_gdf = merge_df_with_gdf(
        geom_gdf, 
        data_info['geom_location_id_header'],
        data_df, 
        data_info['data_location_id_header'],
    )                    
    # if streamflow, add recurrence flow levels of the peak flows, units currently assumed cfs
    # if variable_name == "streamflow":
    #     keep_measures = ['max']
        # if not recurrence_flows_df.empty:
        #     data_gdf = add_recurrence_interval(data_gdf, recurrence_flows_df, flow_col_label = "max")
        #     keep_measures = keep_measures + ['max_recurr_int']        
            
    # if precip, convert to inches/hr, currently in mm/s - for all calculated values/measures returned by query
    all_measures = data_df.columns[~data_df.columns.isin([data_info['data_location_id_header'], 'units'])].to_list() 
    if variable_name == "precipitation_flux":
        #keep_measures = ['sum']        
        for col in all_measures:
            data_gdf[col] = round(data_gdf[col]*60*60, 2)    
            data_gdf[col] = data_gdf[col] / 25.4     
            
    # reduce columns as work around to custom hover tool not working in dynamicmap
    # keep_cols = [data_location_id_header,'geometry','units'] + keep_measures
    # if data_gdf.geom_type.values[0] == 'Point':
    #     keep_cols = keep_cols + ['nwm_feature_id','latitude','longitude','easting','northing']
    # data_gdf = data_gdf.loc[:,keep_cols]   
        
    return data_gdf


def get_historical_timeseries(
    data_info: dict,
    data_location_id_like_string: str, 
    start_value_time: pd.Timestamp = None,
    end_value_time: pd.Timestamp = None,
    variable_name = None,          
) -> pd.DataFrame:
    '''
    query data
    convert/transform/process data
    '''
    data_df = query_historical_data(
        data_info,
        data_location_id_like_string = data_location_id_like_string,
        start_value_time = start_value_time, 
        end_value_time = end_value_time,
        data_type = "timeseries"
    )          
    # if precip, convert values to inches/hr, currently in mm/s
    if variable_name == "precipitation_flux":
        data_df['value'] = round(data_df['value']*60*60, 2)    
        data_df['value'] = data_df['value'] / 25.4      
        data_df['value_cum'] = data_df['value'].cumsum()
        
    return data_df

###### Data processing and organization stuff

def add_recurrence_interval(df_flow, df_recurr, flow_col_label = 'max'):
    '''
    Determine the highest defined recurrence interval flow that was exceeded by the max_flow
    !!! currently assumes column headers are of the format
        "X_0_year_recurrence_flow" and extracts the X value (as in nwm recurrence flow netcdf)
    !!! if there is no 'units' column, units are currently assumed to be CFS
    '''
    # number of locations
    n_locations = len(df_flow)
    
    # get subset of recurrence flows for nwm_features in flow dataframe
    df_recurr_sub = df_recurr.loc[df_flow['nwm_feature_id']]
    recurr_flow_matrix = df_recurr_sub.to_numpy()
    
    # Get the recurrence intervals of the maximum flows

    # create a tiled matrix of the recurrence intervals (years)
    # repeating a row of the interval numbers, nlocations times
    # **currently assumes column headers are of the format
    #   "X_0_year_recurrence_flow" and extracts the X value
    ncol = len(df_recurr_sub.columns)
    recurr_labels = df_recurr_sub.columns.to_list()
    recurr_vals = np.array([int(i.split("_")[0]) for i in recurr_labels])
    recurr_vals_tiled = np.tile(recurr_vals,(n_locations,1))    
    
    # create a tiled matrix of the maximum flow for each reach, 
    # repeating the max_flow column for each column of the recurr
    # flow matrix for comparison     
    flow_data = df_flow[flow_col_label]
              
    # check units, convert flow if needed
    # if 'units' in df_flow.columns:
    #     if df_flow["units"][0].find("ft") < 0:
    #         flow_data =  flow_data / 0.0283
    # else:
    #     print('flow units assumed to be CFS for aligning recurrence intervals')
         
    flow_matrix = np.tile(flow_data,(ncol,1)).transpose() 
    
    # get matrix of where the recurrence flows were exceeded by the max flow
    exceed_recurr_flows = flow_matrix > recurr_flow_matrix

    # Create a new matrix with values equal to the recurr interval value (years)
    # if the max flow exceeded the recurrence flow (exceed_recurr_flows = True)
    recurr_vals_matrix = np.where(exceed_recurr_flows, recurr_vals_tiled, 0)    
    
    # then find the maximum recurr interval exceeded by calculating the max
    # across columns. This is the highest tabulated recurrence interval that 
    # was exceeded by the max flow (i.e., the recurrence category in the 
    # High Water Magnitude Product)
    col_label = 'max_recurr_int'
    df_flow[col_label] = np.amax(recurr_vals_matrix, axis = 1)    
    
    # reorder columns to put max_recurr_int next to max flow
    cols = df_flow.columns.to_list()
    i = cols.index(flow_col_label)
    cols_reordered = cols[:i+1] + cols[-1:] + cols[i+1:-1] 
    df_flow = df_flow[cols_reordered].copy()    
    
    # add flag to indicate that all recurrence flows are equal (i.e., 2-yr flow = 100-yr flow), 
    # likely bad freq. analysis results
    ind = df_recurr.loc[df_recurr.iloc[:,0] == df_recurr.iloc[:,-1]].index.to_list()
    #df_flow.loc[df_flow.index.isin(ind),'qual'] = 'recurr_all_equal'    
    
    return df_flow    


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


def multipolygon_to_polygon_gdf(gdf):
    
    gdf_parsed = gpd.GeoDataFrame()
    for i, polys in enumerate(gdf.geometry):
        row = gdf.loc[[i],:]
        for poly_part in polys.geoms:  
            row['geometry'] = poly_part
            gdf_parsed = pd.concat([gdf, row], axis = 0)
            
    return gdf_parsed


def get_parquet_date_range(source) -> pd.Timestamp:
    '''
    Query parquet files for defined fileset (source) and
    return the min/max value_times in the files
    '''    
    query = f"""
        SELECT count(distinct(value_time)) as count,
        min(value_time) as start_time,
        max(value_time) as end_time
        FROM read_parquet('{source}/*.parquet')
    ;"""
    df = ddb.query(query).to_df()
    return df.start_time[0], df.end_time[0]

##### Widgets

def get_event_date_range_slider(source_list, opts):  
    '''
    Date range slider to select start and end dates of the event
    '''
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

    event_dates_slider = pn.widgets.DatetimeRangeSlider(
        name='Event start/end dates',
        start=start_date, 
        end=end_date,
        # default to start date plus 2 weeks
        value= (start_date, end_date), #start_date + timedelta(days = 14)),
        step=1000*60*60,
        bar_color = 'green',
        width_policy="fit",
        width=600,
    )
    return event_dates_slider

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



###### Plot styling


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

