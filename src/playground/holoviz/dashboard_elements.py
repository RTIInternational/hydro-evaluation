import pandas as pd
import geopandas as gpd
import holoviews as hv
import geoviews as gv
import dashboard_utils as dbu
import dashboard_styles as dbs
from typing import List

def get_historical_chars_geo_element(
    data_info: dict,
    data_location_id_like_string: str, 
    start_value_time: pd.Timestamp = None,
    end_value_time: pd.Timestamp = None,
    variable_name = None,       
    geom_gdf = gpd.GeoDataFrame(),    
    measure: str = None,
    measure_min_requested = None,
    measure_max_requested = None,
    opts = {},
) -> hv.Element:
    '''
    
    '''
    # get data with geometry
    data_gdf = dbu.get_historical_chars_with_geom(
        data_info = data_info,
        data_location_id_like_string = data_location_id_like_string, 
        start_value_time = start_value_time,
        end_value_time = end_value_time,
        variable_name = variable_name,        
        geom_gdf = geom_gdf,               
    )   
    # if mapping the recurrence interval, add recurrence values
    if measure == 'max_recurr_int':
        data_gdf = dbu.add_recurrence_interval(data_gdf, recurrence_flows_df, flow_col_label = "max")   
    if measure == 'max_in_hr':
        data_gdf = data_gdf[data_gdf['upstr_area_km2'].notnull()].copy()
        data_gdf['max_in_hr'] = data_gdf['max'] / (data_gdf['upstr_area_km2']*(1000**2)*(3.28**2)) * 12 * 3600
    
    # subset data based on requested min/max (if any defined, e.g., only > 0 or other threshold)
    if measure_min_requested:
        data_gdf = data_gdf[data_gdf[measure] >= measure_min_requested]
    if measure_max_requested:
        data_gdf = data_gdf[data_gdf[measure] <= measure_max_requested]
    
    # find the actual min/max values of the extracted data for rescaling plots
    measure_min_in_dataset = data_gdf[measure].min()
    measure_max_in_dataset = data_gdf[measure].max()    
          
    #convert to spatialpandas object (required for inspect polygons function)
    data_sdf = spd.GeoDataFrame(data_gdf)   
    
    # check geometry type
    geom_type = data_gdf.geometry.type.iloc[0]
    
    if geom_type == 'Polygon':    
        
        # declare polygon geoviews object           
        #label = f"Mean Areal Precip | {start_value_time} | {end_value_time}"
        map_element_hv = gv.Polygons(
            data_sdf,
            crs=ccrs.GOOGLE_MERCATOR, 
            vdims=[measure, data_info['data_location_id_header']],
            #label = label,
        )  
        map_element_hv.opts(**opts)           
        
    elif geom_type == 'Point':      
        # define data dimensions - more complex for points so plot linkages work
        non_measures = [data_info['geom_location_id_header'], data_info['data_location_id_header'], 
                        'geometry','units','latitude','longitude','easting','northing','nwm_feature_id','upstr_area_km2']
        # custom hover not working, limit to 'measure' column only - possibly make this an argument
        show_all_columns = False
        if show_all_columns:
            all_measures = data_sdf.columns[~data_gdf.columns.isin(non_measures)].to_list()    
        else:
            all_measures = [measure]

        # define dimensions        
        sorted_measures = [measure] + [m for m in all_measures if m!=measure]
        vdims = sorted_measures + [data_info['data_location_id_header'], 'upstr_area_km2']
        kdims = ['easting','northing']
        all_cols_except_geom = vdims + kdims + ['latitude','longitude']

        # leave out geometry - easier to work with the data
        data_df = data_sdf.loc[:,all_cols_except_geom]

        # if mapping the recurrence interval, add recurrence values
        # and sort points so legend appears in order
        if measure == 'max_recurr_int':        
            data_df = data_df.sort_values(measure, ascending = False)     

        # declare points holoviews object   
        label = f"{measure} | {start_value_time} | {end_value_time}"
        map_element_hv = hv.Points(
            data_df, 
            kdims = kdims, 
            vdims = vdims,
            #label = label,
        )
        map_element_hv.opts(**opts, show_legend=False)        
        
        # tooltips = [('ID', '@usgs_site_code'),('Max Flow (cfs)', '@max')]  ###  custom tooltips is not working
        # hover = HoverTool(tooltips=tooltips)            
        # map_element_hv.opts(tools=[hover])

    # reset the data range based on data in the current sample
    map_element_hv.redim.range(**{f"{measure}": (measure_min_in_dataset, measure_max_in_dataset)})  
    #map_element_hv.relabel(label)

    return map_element_hv    

def get_catchment_polygon_hv(
    index: List[int],
    points_dmap: hv.DynamicMap,     
    points_info: dict = {},
    polygons_info: dict = {},
    points_gdf: gpd.GeoDataFrame() = None,
    polygons_gdf: gpd.GeoDataFrame() = None,   
    opts = {},
) -> hv.Element:
    
    if len(index) > 0 and len(points_dmap.dimensions('value')) > 0:    
        point_id = points_dmap.dimension_values(points_info['data_location_id_header'])[index][0]
    else:
        point_id = points_dmap.dimension_values(points_info['data_location_id_header'])[0]
        opts = dict(opts, alpha=0)
        
    polygon_id = points_gdf.loc[points_gdf[points_info['geom_location_id_header']] == point_id, 'catchment_id'].iloc[0]  
    selected_polygon = polygons_gdf.loc[polygons_gdf[polygons_info['geom_location_id_header']]==polygon_id,:]

    polygon_hv = gv.Polygons(selected_polygon, crs=ccrs.GOOGLE_MERCATOR, vdims=[forcing_info['geom_location_id_header']])
    polygon_hv.opts(**opts)     
        
    return polygon_hv


def get_historical_timeseries_ts_element(
    index: List[int],
    points_dmap: hv.DynamicMap,    
    points_info: dict = {},
    polygons_info: dict = {},
    points_gdf: gpd.GeoDataFrame() = None,
    polygons_gdf: gpd.GeoDataFrame() = None,
    variable_name: str = "streamflow", 
    start_value_time: pd.Timestamp = None,
    end_value_time: pd.Timestamp = None,   
    element_type = "curve",
    opts = {},
):
    '''

    '''    
    if len(index) > 0 and len(points_dmap.dimensions('value')) > 0:    
     
        point_id = points_dmap.dimension_values(points_info['data_location_id_header'])[index][0]
        
        if variable_name == "precipitation_flux":
            polygon_id = points_gdf.loc[points_gdf[points_info['geom_location_id_header']] == point_id, 'catchment_id'].iloc[0]  
            title = f"{polygons_info['geom_location_id_header']}: {polygon_id} (Contains Gage: {point_id})"                 
            df = dbu.get_historical_timeseries(
                data_info = polygons_info,
                data_location_id_like_string = polygon_id, 
                start_value_time = event_dates_slider.value_start,
                end_value_time = event_dates_slider.value_end,
                variable_name = variable_name,
            )            
            df['value_time_str'] = df['value_time'].dt.strftime('%Y-%m-%d-%H')
            ymax_bars = max(df['value'].max()*1.1,1)
            ymax_curve = max(df['value_cum'].max()*1.1,1)
            df = df.rename(columns = {'value_cum':'Cumulative (in)'})  # work around to get correct label on secondary axis
            t = start_value_time + (end_value_time - start_value_time)*0.01
            text_x = t.replace(second=0, microsecond=0, minute=0).strftime('%Y-%m-%d-%H')
            text_y = ymax_bars*0.9    

            bars = hv.Bars(df, kdims = [('value_time_str','Date')], vdims = [('value', 'Precip Rate (in/hr)')])
            curve = hv.Curve(df, kdims = [("value_time_str", "Date")], vdims = [('Cumulative (in)', 'Precip (in)')])
            text = hv.Text(text_x, text_y, title).opts(text_align='left', 
                                                       text_font_size='10pt', 
                                                       text_color='#57504d', 
                                                       text_font_style='bold')
        
            bars.opts(**opts, fill_color = 'blue', line_color = None, ylim=(0, ymax_bars))
            curve.opts(**opts, color='orange', hooks=[dbs.plot_secondary_bars_curve])
            
            ts_element_hv = (bars * curve * text).opts(show_title=False)  ##  must control ylim of secondary axis in hook function!
            
            
            #ts_element_hv = bars * curve
            #ts_element_hv.relabel(label)
            
        elif variable_name == "streamflow":
            title = f"Gage ID: {point_id}"
            df = dbu.get_historical_timeseries(
                data_info = points_info,
                data_location_id_like_string = point_id, 
                start_value_time = event_dates_slider.value_start,
                end_value_time = event_dates_slider.value_end,
                variable_name = variable_name,
            )           
            
            upstr_area_km2 = points_dmap.dimension_values('upstr_area_km2')[index][0]
            upstr_area_ft2 = upstr_area_km2*(1000**2)*(3.28**2)
            df["Norm. Flow (in/hr)"] = df['value']/upstr_area_ft2*12*3600
            ymax = max(df['value'].max()*1.1,1)
            t = start_value_time + (end_value_time - start_value_time)*0.01
            text_x = t.replace(second=0, microsecond=0, minute=0)
            text_y = ymax*0.9       

            text = hv.Text(text_x, text_y, title).opts(text_align='left', 
                                                       text_font_size='10pt', 
                                                       text_color='#57504d', 
                                                       text_font_style='bold')
            
            curve_cfs = hv.Curve(df, ("value_time", "Date"), ("value", "Flow (ft3/s)")) 
            curve_in_hr = hv.Curve(df, ("value_time", "Date"), ("Norm. Flow (in/hr)", "Norm. Flow (in/hr)")) 

            curve_cfs.opts(**opts, color='blue', ylim=(0, ymax))
            curve_in_hr.opts(**opts, color='orange', alpha=0, hooks=[dbs.plot_secondary_curve_curve])

            ts_element_hv = (curve_cfs * curve_in_hr * text).opts(show_title=False)  
   
    else:        
        df = pd.DataFrame([[0,0],[1,0]], columns = ['Date','value'])
        label = "Nothing Selected"
        curve = hv.Curve(df, "Date", "value").opts(**opts)
        text = hv.Text(0.01, 0.9, "No Selection").opts(text_align='left', 
                                                       text_font_size='10pt', 
                                                       text_color='#57504d', 
                                                       text_font_style='bold')
        ts_element_hv = (curve * text).opts(show_title=False)
            
    return ts_element_hv      

            
def get_aggregator(measure):
    '''
    datashader aggregator function
    '''
    return ds.mean(measure)