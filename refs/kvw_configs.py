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