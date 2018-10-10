from netCDF4 import default_fillvals as NC_FILL_VALUES

# Valid python netCDF4 NetCDF file types
NETCDF_FORMATS = ['NETCDF3_CLASSIC',
                  'NETCDF4_CLASSIC',
                  'NETCDF4']

# Slocum delayed mode file types
SLOCUM_DELAYED_MODE_EXTENSIONS = ['dbd',
                                  'ebd']

# Slocum real-time mode file types
SLOCUM_REALTIME_MODE_EXTENSIONS = ['sbd',
                                   'mbd',
                                   'nbd',
                                   'tbd']

# Slocum native timestamp sensors   
SLOCUM_TIMESTAMP_SENSORS = ['m_present_time',
                            'sci_m_present_time']

# Slocum native pressure sensors
SLOCUM_PRESSURE_SENSORS = ['sci_water_pressure',
                           'm_water_pressure',
                           'm_pressure']

# Slocum native depth sensors
SLOCUM_DEPTH_SENSORS = ['m_depth']

# Slocum delayed mode file types
SLOCUM_DELAYED_MODE_EXTENSIONS = ['dbd',
                                  'ebd']

# Slocum realtime mode file types
SLOCUM_REALTIME_MODE_EXTENSIONS = ['sbd',
                                   'mbd',
                                   'nbd',
                                   'tbd']

SLOCUM_TEMPERATURE_SENSORS = ['sci_water_temp',
                              'm_water_temp']

SLOCUM_SALINITY_SENSORS = ['sci_water_cond',
                           'm_water_cond']

LLAT_SENSORS = ['llat_time',
                'llat_pressure',
                'llat_latitude',
                'llat_longitude',
                'llat_depth']

NGDAC_VAR_NAMES = ['time', 'depth', 'pressure', 'temperature', 'conductivity', 'salinity', 'density', 'profile_time',
                   'profile_id', 'profile_lat', 'profile_lon', 'lat', 'lon']

REQUIRED_SENSOR_DEFS_KEYS = ['nc_var_name', 'type', 'dimension', 'attrs']
CF_VARIABLE_ATTRIBUTES = ['long_name', 'standard_name', 'units', 'comment']

