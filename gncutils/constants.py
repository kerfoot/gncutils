from netCDF4 import default_fillvals as NC_FILL_VALUES
import datetime
import pytz

# The minimum timestamp allowed to be considered a valid measurement. Used in
# gncutils.readers.slocum.create_llat_dba_reader to eliminate bad timestamps
MIN_TIMESTAMP_ALLOWED = datetime.datetime(2000,1,1,0,0,0).replace(tzinfo=pytz.UTC).timestamp()

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

SCI_CTD_SENSORS = ['llat_latitude',
                   'llat_longitude',
                   'llat_pressure',
                   'sci_water_temp',
                   'sci_water_cond']

ALT_CTD_SENSORS = ['llat_latitude',
                   'llat_longitude',
                   'sci_water_pressure2',
                   'sci_water_temp2',
                   'sci_water_cond2']

M_CTD_SENSORS = ['llat_latitude',
                 'llat_longitude',
                 'llat_pressure',
                 'm_water_temp',
                 'm_water_cond']
