NWM_BUCKET = "national-water-model"

# WKT strings extracted from NWM grids
CONUS_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]], \
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-97.0],PARAMETER["standard_parallel_1",30.0],\
PARAMETER["standard_parallel_2",60.0],PARAMETER["latitude_of_origin",40.0],UNIT["Meter",1.0]]'

HI_NWM_WKT = 'PROJCS["Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-157.42],PARAMETER["standard_parallel_1",10.0],\
PARAMETER["standard_parallel_2",30.0],PARAMETER["latitude_of_origin",20.6],UNIT["Meter",1.0]]'

PR_NWM_WKT = 'PROJCS["Sphere_Lambert_Conformal_Conic",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6370000.0,0.0]],\
PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["false_easting",0.0],\
PARAMETER["false_northing",0.0],PARAMETER["central_meridian",-65.91],PARAMETER["standard_parallel_1",18.1],\
PARAMETER["standard_parallel_2",18.1],PARAMETER["latitude_of_origin",18.1],UNIT["Meter",1.0]]'

# NWM_V22_CONFIGURATIONS = {
#     "analysis_assim":{
#         "implemented": True,
#         "file_name": "analysis_assim",
#         "t": range(0, 24),
#         "tm": range(0, 3),
#         "vars": ["channel_rt", "land", "reservoir","terrain_rt"],
#     },
#     "analysis_assim_extend":{
#         "implemented": True,
#         "t": [16],
#         "tm": range(0, 28),
#         "vars": ["channel_rt", "land", "reservoir","terrain_rt"],
#     },
#     "analysis_assim_extend_no_da":{
#         "implemented": True,
#         "t": [16],
#         "tm": range(0, 28),
#         "vars": ["channel_rt"],
#     },
#     "analysis_assim_hawaii":{
#         "implemented": False,
#     },
#     "analysis_assim_hawaii_no_da":{
#         "implemented": False,
#     },
#     "analysis_assim_long":{
#         "implemented": False,
#     },
#     "analysis_assim_long_no_da":{
#         "implemented": False,
#     },
#     "analysis_assim_no_da":{
#         "implemented": True,
#         "t": range(0, 24),
#         "tm": range(0, 3),
#         "vars": ["channel_rt"],
#     },
#     "analysis_assim_puertorico":{
#         "implemented": False,
#     },
#     "analysis_assim_puertorico_no_da":{
#         "implemented": False,
#     },
#     "forcing_analysis_assim":{
#         "implemented": True,
#         "file_name": "analysis_assim",
#         "t": range(0, 24),
#         "tm": range(0, 3),
#         "vars": ["forcing"],
#         "bands": ["RAINRATE"]
#     },
#     "forcing_analysis_assim_extend":{
#         "t": range(),
#         "tm": range()
#     },
#     "forcing_analysis_assim_hawaii":{
#         "implemented": False,
#     },
#     "forcing_analysis_assim_puertorico":{
#         "implemented": False,
#     },
#     "forcing_medium_range":{
#         "t": range(),
#         "tm": range()
#     },
#     "forcing_short_range":{
#         "t": range(),
#         "tm": range()
#     },
#     "forcing_short_range_hawaii":{
#         "implemented": False,
#     },
#     "forcing_short_range_puertorico":{
#         "implemented": False,
#     },
#     "long_range_mem1":{
#         "implemented": False,
#     },
#     "long_range_mem2":{
#         "implemented": False,
#     },
#     "long_range_mem3":{
#         "implemented": False,
#     },
#     "long_range_mem4":{
#         "implemented": False,
#     },
#     "medium_range_mem1":{
#         "t": range(),
#         "tm": range()
#     },
#     "medium_range_mem2":{
#         "t": range(),
#         "tm": range()
#     },
#     "medium_range_mem3":{
#         "t": range(),
#         "tm": range()
#     },
#     "medium_range_mem4":{
#         "t": range(),
#         "tm": range()
#     },
#     "medium_range_mem5":{
#         "t": range(),
#         "tm": range()
#     },
#     "medium_range_mem6":{
#         "t": range(),
#         "tm": range()
#     },
#     "medium_range_mem7":{
#         "t": range(),
#         "tm": range()
#     },
#     "medium_range_no_da":{
#         "implemented": False,
#     },
#     "short_range":{
#         "t": range(),
#         "tm": range()
#     },
#     "short_range_hawaii":{
#         "implemented": False,
#     },
#     "short_range_hawaii_no_da":{
#         "implemented": False,
#     },
#     "short_range_puertorico":{
#         "implemented": False,
#     },
#     "short_range_puertorico_no_da":{
#         "implemented": False,
#     }
# }