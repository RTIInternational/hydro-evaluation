def config_specs(config, domain, version, member=1):
    '''
    Build a dataframe of NWM specifications for a defined configuration,
    accounting for differences between domains and versions, includng
        - dir_suffix:       suffix appended to configuration directory, e.g. _hawaii, _puertorico, _mem1
        - var_str_suffix    suffix appended to variable string in filename, e.g. channel_rt_1 
        - duration_hours    simulation period in hours
        - timestep_int      simulation timestep in hours (fraction if < 1 hour)
        - runs_per_day      number of executions per day
        - is_forecast       True if forecast, False if AnA
        - abbrev            short abbrevation for use in column headers and titles
    '''
    
    # if running latest_ana mode, start with standard AnA config
    if config == 'latest_ana':
        config = 'analysis_assim'
    
    # first build the base dataframe of config info for all configs
    # note that for medium range, assume only evaluating 'member 1' for now
    
    if domain == 'conus':
    
        df_config_specs = pd.DataFrame(
            {"dir_suffix" : ["", "_mem1", "", ""],
             "var_str_suffix" : ["","_1","",""],
             "duration_hrs" : [18, 240, 3, 28], 
             "timestep_int" : [1, 1, 1, 1], 
             "runs_per_day" : [24, 4, 24, 1],
             "base_run_hour" : [0, 0, 0, 16],
             "latency" : [1.5, 6, 0.5, 3],
             "is_forecast" : [True, True, False, False],
             "abbrev" : ['srf','mrf','stana','exana']},
            index = ["short_range", "medium_range", "analysis_assim", "analysis_assim_extend"])    
            
        if version < 2.1:
            # in v2.0, medium range time step was 3 hours, changed to 1 hour in v2.1
            df_config_specs.loc['medium_range','timestep_int'] = 3
            
        # for medium range members 2-7, change suffixes and duration 
        if config == 'medium_range' and member > 1:
            df_config_specs.loc['medium_range','duration_hrs'] = 204
            df_config_specs.loc['medium_range','dir_suffix'] = "_mem" + str(member)
            df_config_specs.loc['medium_range','var_str_suffix'] = "_" + str(member)    
            
    elif domain == 'hawaii':
    
        # exit if the configuration requested that does not exist
        if config in ['medium_range','long_range','analysis_assim_extend']:
            raise ValueError(f'Config {config} does not exist for domain {domain}') 
        
        # hawaii short range extends out 60 hours in 2.0 and 48 hours in 2.1
        # hawaii timesteps changes from 1 hour in 2.0 to 15 min in 2.1 (both SRF and AnA)
        # hawaii run interval changes from 6 hours (4 x day) in 2.0 to 12 hours (2 x day) in 2.1
        df_config_specs = pd.DataFrame(
            {"dir_suffix" : ['_' + domain, '_' + domain],
             "var_str_suffix" : ["",""],
             "duration_hrs" : [48, 3], 
             "timestep_int" : [0.25, 0.25], 
             "runs_per_day" : [2, 24],
             "base_run_hour" : [0, 0],
             "latency" : [1.5, 0.5],
             "is_forecast" : [True, False],
             "abbrev" : ['srf','stana']},
            index = ["short_range", "analysis_assim"])
            
        if version == 2.0:
            df_config_specs.loc[:,'timestep_int'] = [1, 1]
            df_config_specs.loc['short_range','duration_hrs'] = 60
            df_config_specs.loc['short_range','runs_per_day'] = 4                
        
    # puerotrico does not exist prior to v2.1
    elif domain == 'puertorico':

        # exit if the configuration requested that does not exist
        if config in ['medium_range','long_range','analysis_assim_extend']:
            raise ValueError(f'Config {config} does not exist for domain {domain}')

        if version < 2.1:
            raise ValueError(f'Domain {domain} does not exist for version {version}')      
            
        df_config_specs = pd.DataFrame(
            {"dir_suffix" : ['_' + domain, '_' + domain],
             "var_str_suffix" : ["",""],
             "duration_hrs" : [48, 3], 
             "timestep_int" : [1, 1], 
             "runs_per_day" : [2, 24],              # runs at 6 hr and 18 hr - code updates needed for this!
             "base_run_hour" : [6, 0],
             "latency" : [1.5, 0.5],
             "is_forecast" : [True, False],
             "abbrev" : ['srf','stana']},
            index = ["short_range", "analysis_assim"])
              
    # reduce dataframe to the row for specified configuration
    df_config_specs = df_config_specs.loc[[config]]
    
    return df_config_specs
    
    
    
def variable_specs(domain):
    '''
    Build a dataframe of filename specifications that differ by variable 
        dir_prefix:         prefix to the configuration directory, i.e., forcing_short_range
        use_suffix:         turn on or off dir_suffix, defined in config specs     
        var_string:         variable string in filename
        var_out_units       units of data
    '''
        
    # build dataframe of variable group info and processing flags
    df_var_specs = pd.DataFrame(
            {"dir_prefix" : ["forcing_", ""], 
             "use_suffix" : [False, True], 
             "var_string" : ["forcing", "channel_rt"],
             "var_out_units" : ["mm hr-1","cms"]},
            index = ["forcing", "channel"])
 
    # adjustments to base info for hawaii domain and version 
    #(for v2.1 will add other domain specifics, PR, AK)
    if domain == 'hawaii':
        
        # turn on flag to add domain suffix (defined in config specs)
        # for both variables (in conus used for medium term "mem1" suffix, 
        # med term does not exist for Hawaii and suffix instead indicates "hawaii"
        df_var_specs['use_suffix'] = [True, True]                          

    return df_var_specs
    
    
    
def nwm_version(ref_time):
    '''
    Get NWM version (2.0 or 2.1) corresponding to the reference time     
        - v2.0 assumed for all datetimes prior to 4-20-2021 13z
        - v2.1 begins 4-20 at 14z 
        - v2.2 begins 7-09 at 00z 
        - *code update would be needed for versions prior to 2.0
    '''    
    
    v21_date = datetime(2021, 4, 20, 14, 0, 0)
    v22_date = datetime(2022, 7, 9, 0, 0, 0)
    
    if ref_time >= v22_date:
        version = 2.2
    elif ref_time >= v21_date:
        version = 2.1
    else:
        version = 2.0
        
    return version
    
    
    
def get_ana_fileparts(start_time, config, n_files, ts_int, df_parts, include_t0, use_tm02 = True, is_latest = False):
    '''
    Build a dataframe of filename parts for an AnA configuration based on argument specifications
    '''
    
    #get current clock time to check if "next day AnA" is available yet
    clock_ztime = datetime.utcnow().replace(second=0, microsecond=0, minute=0)    

    for i in range(n_files):
    
        # get ts_hr - the time of this timestep iteration relative to the start time
        # when T0 not included (will only occur when aligning with a forecast), shift ts_hr forward 
        if include_t0:
            ts_hr = i*ts_int 
        else:
            ts_hr = (i+1)*ts_int       
               
        # get the valid time of the timestep for this iteration
        val_time = start_time + timedelta(hours=ts_hr) # calendar date/time of forecast timestep

        if config == "analysis_assim_extend":
            #e-AnA only runs in 16z cycle, get all output from this ref-time, either current or next date
            ref_hr_string = '16z'  
            
            #Valid hours 0-12 --> align with tm16-tm04 in same date directory
            if val_time.hour < 13:  
                datedir = val_time.strftime("nwm.%Y%m%d")
                ts_hr = 16 - val_time.hour            
                
            #Valid hours 13-23 --> align with tm27-tm17 in next date directory    
            else:              
                nextday = (val_time + timedelta(days=1)).replace(second=0, microsecond=0, minute=0)
                datedir = (nextday).strftime("nwm.%Y%m%d")
                ts_hr = 40 - val_time.hour
                              
                # if nextday not yet available, fill in tm03-00 from current day
                if nextday.replace(hour=19) > clock_ztime:
                    datedir = val_time.strftime("nwm.%Y%m%d")
                    ts_hr = 16 - val_time.hour
                    
                    # if the ts_hr becomes negative, use next day - will be missing but keep in the filelist
                    if ts_hr < 0:
                        ts_hr = 40 - val_time.hour
                        datedir = (nextday).strftime("nwm.%Y%m%d")
             
        else:
            #standard AnA runs every cycle, if get_tm02 = True, use tm02 from ref_time + 2, else use tm00 from ref-time
            if use_tm02:
                datedir = (val_time + timedelta(hours=2)).strftime("nwm.%Y%m%d")
                ref_hr_string = (val_time + timedelta(hours=2)).strftime("%Hz")
                ts_hr = 2
                
                val_minutes = val_time.minute
                if val_minutes > 0:
                    ts_min = 60 - val_minutes
                else:
                    ts_min = 0
                
                # if building most recent possible filelist, end with T0
                if is_latest:
                
                    # 2nd to last file is T1 of most recent available reftime
                    if i >= (n_files - (1/ts_int) - 1):                      
                        datedir = (val_time + timedelta(hours=1)).strftime("nwm.%Y%m%d")
                        ref_hr_string = (val_time + timedelta(hours=1)).strftime("%Hz")                      
                        ts_hr = 1
   
                    # last file is T0 of most recent available reftime
                    if i == n_files - 1:
                        datedir = val_time.strftime("nwm.%Y%m%d")
                        ref_hr_string = val_time.strftime("%Hz")
                        ts_hr = 0    
                
                # if valid time is not top of the hour, need to subtract an hour to get the 
                # corresponding AnA timestep string, since they increase going backward in time
                if val_minutes > 0:
                    ts_hr = ts_hr - 1
                
            # if get_tm02 is False, use the t00 from each standard AnA (currently never used)
            # ** this option does not yet work if timestep interval (ts_int) is < 1 hour
            else:
                datedir = val_time.strftime("nwm.%Y%m%d")
                ref_hr_string = val_time.strftime("%Hz")
                ts_hr = 0
                  
        # update filename parts dataframe
        df_parts.loc[i,'datedir'] = datedir
        df_parts.loc[i,'ref_hr_string'] = 't' + ref_hr_string    
        df_parts.loc[i,'ts_hr_string'] = 'tm' + str(ts_hr).zfill(2)
        df_parts.loc[i,'val_time'] = val_time
        
        # if using this function to get the latest AnA, the config is always standard AnA 
        if is_latest:
            df_parts.loc[i,'config'] = 'analysis_assim'
        
        # if ts_int is a fraction (hawaii is 15 min), add minutes to the time string
        if ts_int%1 > 0:
            df_parts.loc[i,'ts_hr_string'] = 'tm' + str(ts_hr).zfill(2) + str(ts_min).zfill(2)
        
    return df_parts