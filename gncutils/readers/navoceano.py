import argparse
import os
import logging
import numpy as np
from netCDF4 import Dataset


logger = logging.getLogger(os.path.basename(__name__))


def create_llat_nc_reader(nc_file):

    parser = {}

    if not os.path.isfile(nc_file):
        logger.error('Invalid NetCDF file specified: {:s}'.format(nc_file))
        return parser

    # Parse the NAVOCEANO NetCDF file
    parser = parse_nc_file(nc_file)

    # Replace time, depth, pressure, lat and lon with their llat variable names
    sensors = [s['sensor_name'] for s in parser['sensors']]
    if 'time' not in sensors:
        logger.error('No time variable found: {:s}'.format(nc_file))
        return
    if 'depth' not in sensors:
        logger.error('No depth variable found: {:s}'.format(nc_file))
        return
    if 'pressure' not in sensors:
        logger.error('No pressure variable found: {:s}'.format(nc_file))
        return
    if 'latitude' not in sensors:
        logger.error('No latitude variable found: {:s}'.format(nc_file))
        return
    if 'longitude' not in sensors:
        logger.error('No longitude variable found: {:s}'.format(nc_file))
        return

    # Use NAVO scitime variable for the time coordinate
    i = sensors.index('scitime')
    parser['sensors'][i]['sensor_name'] = 'llat_time'
    i = sensors.index('depth')
    parser['sensors'][i]['sensor_name'] = 'llat_depth'
    i = sensors.index('pressure')
    parser['sensors'][i]['sensor_name'] = 'llat_pressure'
    i = sensors.index('latitude')
    parser['sensors'][i]['sensor_name'] = 'llat_latitude'
    i = sensors.index('longitude')
    parser['sensors'][i]['sensor_name'] = 'llat_longitude'

    return parser


def parse_nc_file(nc_file):

    parser = {}

    if not os.path.isfile(nc_file):
        logger.error('Invalid NetCDF file specified: {:s}'.format(nc_file))
        return parser

    # Parse file metadata
    metadata = get_nc_file_metadata(nc_file)
    if not metadata:
        return parser

    # Parse time series variables metadata
    sensor_defs = get_nc_timeseries_variable_defs(nc_file)
    if not sensor_defs:
        return parser

    # Grab the data
    data = load_nc_timeseries_data(nc_file)
    if data is None:
        return parser

    parser['file_metadata'] = metadata
    parser['sensors'] = sensor_defs
    parser['data'] = data

    return parser


def get_nc_file_metadata(nc_file):

    file_metadata = {}

    if not os.path.isfile(nc_file):
        logger.error('Invalid NetCDF file specified: {:s}'.format(nc_file))
        return file_metadata

    try:
        nci = Dataset(nc_file, 'r')
    except IOError as e:
        logger.error('Erroring reading NAVOCEANO NetCDF {:s}: {:}'.format(nc_file, e))
        return file_metadata

    for att in nci.ncattrs():
        file_metadata[att] = nci.getncattr(att)

    file_metadata['source_file'] = os.path.basename(nc_file)

    # Add the source NetCDF file type
    file_metadata['netcdf_model'] = nci.data_model

    return file_metadata


def get_nc_timeseries_variable_defs(nc_file):

    sensor_defs = []

    if not os.path.isfile(nc_file):
        logger.error('Invalid NetCDF file specified: {:s}'.format(nc_file))
        return sensor_defs

    try:
        nci = Dataset(nc_file, 'r')
    except IOError as e:
        logger.error('Erroring reading NAVOCEANO NetCDF {:s}: {:}'.format(nc_file, e))
        return sensor_defs

    # Find the time dimension
    if 'time' not in nci.dimensions:
        logger.warning('NetCDF file does not contain a time dimension: {:s}'.format(nc_file))
        return sensor_defs

    for var_name, var in nci.variables.items():

        # time must be the only dimension
        var_dims = var.dimensions
        if len(var_dims) != 1:
            continue

        if var_dims[0] != 'time':
            continue

        sensor_def = {'attrs': {}, 'sensor_name': var_name}
        for att in var.ncattrs():
            sensor_def['attrs'][att] = var.getncattr(att)

        sensor_def['attrs']['source_sensor'] = var_name

        sensor_defs.append(sensor_def)

    return sensor_defs


def get_nc_variable_defs(nc_file):

    sensor_defs = []

    if not os.path.isfile(nc_file):
        logger.error('Invalid NetCDF file specified: {:s}'.format(nc_file))
        return sensor_defs

    try:
        nci = Dataset(nc_file, 'r')
    except IOError as e:
        logger.error('Erroring reading NAVOCEANO NetCDF {:s}: {:}'.format(nc_file, e))
        return sensor_defs

    for var_name, var in nci.variables.items():

        sensor_def = {'attrs': {}, 'sensor_name': var_name}
        for att in var.ncattrs():
            sensor_def['attrs'][att] = var.getncattr(att)

        sensor_def['attrs']['source_sensor'] = var_name

        sensor_defs.append(sensor_def)

    return sensor_defs


def load_nc_timeseries_data(nc_file):

    data = []

    if not os.path.isfile(nc_file):
        logger.error('Invalid NetCDF file specified: {:s}'.format(nc_file))
        return data

    try:
        nci = Dataset(nc_file, 'r')
    except IOError as e:
        logger.error('Erroring reading NAVOCEANO NetCDF {:s}: {:}'.format(nc_file, e))
        return data

    # Find the time dimension
    if 'time' not in nci.dimensions:
        logger.warning('NetCDF file does not contain a time dimension: {:s}'.format(nc_file))
        return data

    dim_length = nci.dimensions['time'].size

    # Initialize the data array
    data = np.empty((dim_length,0))

    for var_name, var in nci.variables.items():

        # time must be the only dimension
        var_dims = var.dimensions
        if len(var_dims) != 1:
            continue

        if var_dims[0] != 'time':
            continue

        # Append it column wise
        data = np.append(data, np.expand_dims(var[:], axis=1), axis=1)

    return data


