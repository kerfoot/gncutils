"""Routines for parsing Slocum glider ascii dba files created with the
shoreside executables provided by Teledyne Webb Research.  These routines will
not parse Slocum matlab (.m,.dat) files produced by dba2_orig_matlab or
dba2_glider_data"""

import os
import logging
import numpy as np
import datetime
from copy import deepcopy
from gsw import z_from_p as gsw_z_from_p
from gncutils.gps import interpolate_gps, get_decimal_degrees
from gncutils.constants import (
    SLOCUM_TIMESTAMP_SENSORS,
    SLOCUM_PRESSURE_SENSORS,
    SLOCUM_DEPTH_SENSORS,
    SCI_CTD_SENSORS,
    M_CTD_SENSORS,
    ALT_CTD_SENSORS)
from gncutils.ctd import calculate_practical_salinity, calculate_density, calculate_sound_speed
from gsw import pt0_from_t

logger = logging.getLogger(os.path.basename(__name__))


def create_llat_dba_reader(dba_file, timesensor=None, pressuresensor=None, depthsensor=None, z_from_p=True):
    if not os.path.isfile(dba_file):
        logging.error('dba file does not exist: {:s}'.format(dba_file))
        return

    # Parse the dba file    
    dba = parse_dba(dba_file)
    if not dba:
        return

    # List of available dba sensors
    dba_sensors = [s['sensor_name'] for s in dba['sensors']]

    # Select the time sensor
    time_sensor = select_time_sensor(dba, timesensor=timesensor)
    if not time_sensor:
        return
    # Select the pressure sensor
    pressure_sensor = select_pressure_sensor(dba, pressuresensor=pressuresensor)
    # Select the depth sensor
    depth_sensor = select_depth_sensor(dba, depthsensor=depthsensor)
    # We must have either a pressure_sensor or depth_sensor to continue
    if not pressure_sensor and not depth_sensor:
        logger.warning('No pressure sensor and no depth sensor found: {:s}'.format(dba_file))
        return

    # Must have m_gps_lat and m_gps_lon to convert to decimal degrees  
    if 'm_gps_lat' not in dba_sensors or 'm_gps_lon' not in dba_sensors:
        logger.warning('Missing m_gps_lat and/or m_gps_lon: {:s}'.format(dba_file))
    else:
        # Convert m_gps_lat to decimal degrees and create the new sensor definition
        c = dba_sensors.index('m_gps_lat')
        lat_sensor = deepcopy(dba['sensors'][c])
        lat_sensor['sensor_name'] = 'llat_latitude'
        lat_sensor['attrs']['source_sensor'] = u'm_gps_lat'
        lat_sensor['attrs']['comment'] = u'm_gps_lat converted to decimal degrees and interpolated'
        lat_sensor['data'] = np.empty((len(dba['data']), 1)) * np.nan
        for x in range(len(dba['data'])):
            # Skip default values (69696969)
            if abs(dba['data'][x, c]) > 9000:
                continue
            lat_sensor['data'][x] = get_decimal_degrees(dba['data'][x, c])

        # Convert m_gps_lon to decimal degrees and create the new sensor definition
        c = dba_sensors.index('m_gps_lon')
        lon_sensor = deepcopy(dba['sensors'][c])
        lon_sensor['sensor_name'] = 'llat_longitude'
        lon_sensor['attrs']['source_sensor'] = u'm_gps_lon'
        lon_sensor['attrs']['comment'] = u'm_gps_lon converted to decimal degrees and interpolated'
        lon_sensor['data'] = np.empty((len(dba['data']), 1)) * np.nan
        for x in range(len(dba['data'])):
            # Skip default values (69696969)
            if abs(dba['data'][x, c]) > 18000:
                continue
            lon_sensor['data'][x] = get_decimal_degrees(dba['data'][x, c])

    # Interpolate llat_latitude and llat_longitude
    lat_sensor['data'], lon_sensor['data'] = interpolate_gps(time_sensor['data'], lat_sensor['data'],
                                                             lon_sensor['data'])

    # If no depth_sensor was selected, use llat_latitude, llat_longitude and llat_pressure to calculate
    if not depth_sensor or z_from_p:
        if pressure_sensor:
            logger.debug(
                'Calculating depth from selected pressure sensor: {:s}'.format(
                    pressure_sensor['attrs']['source_sensor']))

            depth_sensor = {'sensor_name': 'llat_depth',
                            'attrs': {}}
            depth_sensor['attrs']['source_sensor'] = 'llat_pressure,llat_latitude'
            depth_sensor['attrs']['comment'] = u'Calculated from llat_pressure and llat_latitude using gsw.z_from_p'
            depth_sensor['data'] = -gsw_z_from_p(pressure_sensor['data'], lat_sensor['data'])
        else:
            logging.warning('No pressure sensor found for calculating depth')

    # Append the llat variables
    dba['data'] = np.append(dba['data'], time_sensor['data'], axis=1)
    del (time_sensor['data'])
    dba['sensors'].append(time_sensor)
    if pressure_sensor:
        dba['data'] = np.append(dba['data'], pressure_sensor['data'], axis=1)
        del (pressure_sensor['data'])
        dba['sensors'].append(pressure_sensor)
    dba['data'] = np.append(dba['data'], depth_sensor['data'], axis=1)
    del (depth_sensor['data'])
    dba['sensors'].append(depth_sensor)
    dba['data'] = np.append(dba['data'], lat_sensor['data'], axis=1)
    del (lat_sensor['data'])
    dba['sensors'].append(lat_sensor)
    dba['data'] = np.append(dba['data'], lon_sensor['data'], axis=1)
    del (lon_sensor['data'])
    dba['sensors'].append(lon_sensor)

    # Remove timestamps = 0 for time_llat
    sensors = [s['sensor_name'] for s in dba['sensors']]
    data = dba['data']
    if 'llat_time' in sensors:
        i = sensors.index('llat_time')
        good_rows = data[:, i] > 1
        if data.shape[0] != good_rows.sum():
            logging.warning('Removing {:} bad timestamps'.format(data.shape[0] - good_rows.sum()))
            dba['data'] = data[good_rows, :]

    return dba


def parse_dba(dba_file):
    """Parse a Slocum dba ascii table file.
    
    Args:
        dba_file: dba file to parse
        
    Returns:
        A dictionary containing the file metadata, sensor defintions and data
    """

    if not os.path.isfile(dba_file):
        logging.error('dba file does not exist: {:s}'.format(dba_file))
        return

    # Parse the dba header
    dba_headers = parse_dba_header(dba_file)
    if not dba_headers:
        return

    # Parse the dba sensor definitions
    sensor_defs = parse_dba_sensor_defs(dba_file)
    if not sensor_defs:
        return

    # Add the full path to the dba_file
    dba_headers['source_file'] = os.path.realpath(dba_file)
    # Add the dba file size
    dba_stat = os.stat(dba_file)
    dba_headers['file_size_bytes'] = dba_stat.st_size
    # Parse the ascii table portion of the dba file
    data = load_dba_data(dba_file)
    if not len(data):
        return

    dba = {'file_metadata': dba_headers,
           'sensors': sensor_defs,
           'data': data}

    return dba


def parse_dba_header(dba_file):
    """Parse the header information in a Slocum dba ascii table file.  All header
    lines of the format 'key: value' are parsed.
    
    Args:
        dba_file: dba file to parse
        
    Returns:
        A dictionary containing the file metadata
    """

    if not os.path.isfile(dba_file):
        logging.error('Invalid dba file: {:s}'.format(dba_file))
        return

    try:
        with open(dba_file, 'r') as fid:
            dba_headers = {}
            for f in fid:

                tokens = f.strip().split(': ')
                if len(tokens) != 2:
                    break

                dba_headers[tokens[0]] = tokens[1]
    except IOError as e:
        logging.error('Error parsing {:s} dba header: {}'.format(dba_file, e))
        return

    if not dba_headers:
        logging.warning('No headers parsed: {:s}'.format(dba_file))

    return dba_headers


def parse_dba_sensor_defs(dba_file):
    """Parse the sensor definitions in a Slocum dba ascii table file.
    
    Args:
        dba_file: dba file to parse
        
    Returns:
        An array of dictionaries containing the file sensor definitions
    """

    if not os.path.isfile(dba_file):
        logging.error('Invalid dba file: {:s}'.format(dba_file))
        return {}

    # Parse the file header lines
    dba_headers = parse_dba_header(dba_file)
    if not dba_headers:
        return

    if 'num_ascii_tags' not in dba_headers:
        logging.warning('num_ascii_tags header missing: {:s}'.format(dba_file))
        return

    # Sensor definitions begin on the line number after that contained in the
    # dba_headers['num_ascii_tags']
    num_header_lines = int(dba_headers['num_ascii_tags'])

    try:
        with open(dba_file, 'r') as fid:
            sensors = []
            line_count = 0
            while line_count < num_header_lines:
                fid.readline()
                line_count += 1

            # Get the sensor names line
            sensors_line = fid.readline().strip()
            # Get the sensor units line
            units_line = fid.readline().strip()
            # Get the datatype byte storage information
            bytes_line = fid.readline().strip()
    except IOError as e:
        logging.error('Error parsing {:s} dba header: {:s}'.format(dba_file, e))

    sensors = sensors_line.split()
    units = units_line.split()
    datatype_bytes = bytes_line.split()

    if not sensors:
        logging.warning('No sensor defintions parsed: {:s}'.format(dba_file))
        return

    return [{'sensor_name': sensors[i],
             'attrs': {'units': units[i], 'bytes': int(datatype_bytes[i]), 'source_sensor': sensors[i],
                       'long_name': sensors[i]}} for i in
            range(len(sensors))]


def load_dba_data(dba_file):
    if not os.path.isfile(dba_file):
        logging.error('Invalid dba file: {:s}'.format(dba_file))
        return

    # Parse the dba header
    dba_headers = parse_dba_header(dba_file)
    if not dba_headers:
        return

    # Figure out what line the data table begins on using the header's
    # num_ascii_tags + num_lable_lines 
    if 'num_ascii_tags' not in dba_headers:
        logger.warning('num_ascii_tags header missing: {:s}'.format(dba_file))
        return
    if 'num_label_lines' not in dba_headers:
        logger.warning('num_label_lines header missing: {:s}'.format(dba_file))
        return
    num_header_lines = int(dba_headers['num_ascii_tags'])
    num_label_lines = int(dba_headers['num_label_lines'])
    # Total number of header lines before the data matrix starts
    total_header_lines = num_header_lines + num_label_lines

    ## Parse the sensor header lines
    # sensor_defs = parse_dba_sensor_defs(dba_file)
    # if not sensor_defs:
    #    return

    # Use numpy.loadtxt to load the ascii table, skipping header rows and requiring
    # a 2-D output array
    try:
        t0 = datetime.datetime.utcnow()
        data_table = np.loadtxt(dba_file, skiprows=total_header_lines, ndmin=2)
        t1 = datetime.datetime.utcnow()
        elapsed_time = t1 - t0
        logger.debug('DBD parsed in {:0.0f} seconds'.format(elapsed_time.total_seconds()))
    except ValueError as e:
        logger.warning('Error parsing {:s} ascii data table: {:s}'.format(dba_file, e))
        return

    return data_table


def select_time_sensor(dba, timesensor=None):
    # List of available dba sensors
    dba_sensors = [s['sensor_name'] for s in dba['sensors']]

    # Figure out which time sensor to select
    time_sensor = None
    if timesensor:
        if timesensor not in dba_sensors:
            logger.warning('Specified timesensor {:s} not found in dba: {:s}'.format(timesensor, dba['file_metadata'][
                'source_file']))
            return

        c = dba_sensors.index(timesensor)
        time_sensor = deepcopy(dba['sensors'][c])
        time_sensor['sensor_name'] = 'llat_time'
        time_sensor['attrs']['source_sensor'] = timesensor
        time_sensor['attrs']['comment'] = u'Alias for {:s}'.format(timesensor)
    else:
        for t in SLOCUM_TIMESTAMP_SENSORS:
            if t in dba_sensors:
                c = dba_sensors.index(t)
                time_sensor = deepcopy(dba['sensors'][c])
                time_sensor['sensor_name'] = 'llat_time'
                time_sensor['attrs']['source_sensor'] = t
                time_sensor['attrs']['comment'] = u'Alias for {:s}'.format(t)
                break

    time_sensor['attrs']['units'] = 'seconds since 1970-01-01 00:00:00Z'
    if not time_sensor:
        return

    # Add the sensor data to time_sensor
    time_sensor['data'] = np.expand_dims(dba['data'][:, c], 1)
    # dba['data'] = np.concatenate((dba['data'], time), axis=1)

    return time_sensor


def select_pressure_sensor(dba, pressuresensor=None):
    """Returns selected pressure sensor name and pressure array in decibars"""

    # List of available dba sensors
    dba_sensors = [s['sensor_name'] for s in dba['sensors']]

    # User pressuresensor if specified
    pressure_sensor = None
    if pressuresensor:
        if pressuresensor not in dba_sensors:
            logger.warning('Specified pressuresensor {:s} not found in dba: {:s}'.format(pressuresensor,
                                                                                         dba['file_metadata'][
                                                                                             'source_file']))
            return

        c = dba_sensors.index(pressuresensor)
        pressure_sensor = deepcopy(dba['sensors'][c])
        pressure_sensor['sensor'] = 'llat_pressure'
        pressure_sensor['attrs']['source_sensor'] = pressuresensor
        pressure_sensor['attrs']['comment'] = u'Alias for {:s}, multiplied by 10 to convert from bar to dbar'.format(
            pressuresensor)
    else:
        for p in SLOCUM_PRESSURE_SENSORS:
            if p in dba_sensors:
                c = dba_sensors.index(p)
                pressure_sensor = deepcopy(dba['sensors'][c])
                pressure_sensor['sensor_name'] = 'llat_pressure'
                pressure_sensor['attrs']['source_sensor'] = p
                pressure_sensor['attrs'][
                    'comment'] = u'Alias for {:s}, multiplied by 10 to convert from bar to dbar'.format(p)
                break

    if not pressure_sensor:
        return

    # Add the sensor data to pressure_sensor
    pressure_sensor['data'] = np.expand_dims(dba['data'][:, c] * 10, 1)
    pressure_sensor['attrs']['units'] = 'dbar'

    return pressure_sensor


def select_depth_sensor(dba, depthsensor=None):
    # List of available dba sensors
    dba_sensors = [s['sensor_name'] for s in dba['sensors']]

    # User pressuresensor if specified
    depth_sensor = None
    if depthsensor:
        if depthsensor not in dba_sensors:
            logger.warning('Specified depthsensor {:s} not found in dba: {:s}'.format(depthsensor, dba['file_metadata'][
                'source_file']))
            return

        c = dba_sensors.index(depthsensor)
        depth_sensor = deepcopy(dba['sensors'][c])
        depth_sensor['sensor_name'] = 'llat_depth'
        depth_sensor['attrs']['source_sensor'] = depthsensor
        depth_sensor['attrs']['comment'] = u'Alias for {:s}'.format(depthsensor)
    else:
        for d in SLOCUM_DEPTH_SENSORS:
            if d in dba_sensors:
                c = dba_sensors.index(d)
                depth_sensor = deepcopy(dba['sensors'][c])
                depth_sensor['sensor_name'] = 'llat_depth'
                depth_sensor['attrs']['source_sensor'] = d
                depth_sensor['attrs']['comment'] = u'Alias for {:s}'.format(d)
                break

    if not depth_sensor:
        return

    # Add the sensor data to depth_sensor
    depth_sensor['data'] = np.expand_dims(dba['data'][:, c], 1)

    return depth_sensor


def derive_ctd_parameters(dba):

    # Get the list of sensors parsed from the dba_file
    dba_sensors = [s['sensor_name'] for s in dba['sensors']]

    ctd_sensors = []
    found_ctd_sensors = [s for s in SCI_CTD_SENSORS if s in dba_sensors]
    if len(found_ctd_sensors) == len(SCI_CTD_SENSORS):
        ctd_sensors = SCI_CTD_SENSORS
    else:
        found_ctd_sensors = [s for s in M_CTD_SENSORS if s in dba_sensors]
        if len(found_ctd_sensors) == len(M_CTD_SENSORS):
            ctd_sensors = M_CTD_SENSORS

    if not ctd_sensors:
        logging.warning('Required CTD sensors not found in {:s}'.format(dba['file_metadata']['source_file']))
        return dba
    else:
        lati = dba_sensors.index(ctd_sensors[0])
        loni = dba_sensors.index(ctd_sensors[1])
        pi = dba_sensors.index(ctd_sensors[2])
        ti = dba_sensors.index(ctd_sensors[3])
        ci = dba_sensors.index(ctd_sensors[4])

        lat = dba['data'][:, lati]
        lon = dba['data'][:, loni]
        p = dba['data'][:, pi]
        t = dba['data'][:, ti]
        c = dba['data'][:, ci]

        # Calculate salinity
        salt = calculate_practical_salinity(c, t, p)
        # Calculate density
        density = calculate_density(t, p, salt, lat, lon)
        # Calculate potential temperature
        p_temp = pt0_from_t(salt, t, p)

        # Add parameters as long as they exist in ncw.nc_sensor_defs
        # if 'salinity' in ncw.nc_sensor_defs:
        sensor_def = {'sensor_name': 'salinity',
                      'attrs': {'ancillary_variables': 'conductivity,temperature,presssure',
                                'observation_type': 'calculated'}}
        dba['data'] = np.append(dba['data'], np.expand_dims(salt, 1), axis=1)
        dba['sensors'].append(sensor_def)

        # if 'density' in ncw.nc_sensor_defs:
        sensor_def = {'sensor_name': 'density',
                      'attrs': {
                          'ancillary_variables': 'conductivity,temperature,presssure,latitude,longitude',
                          'observation_type': 'calculated'}}
        dba['data'] = np.append(dba['data'], np.expand_dims(density, 1), axis=1)
        dba['sensors'].append(sensor_def)

        # if 'potential_temperature' in ncw.nc_sensor_defs:
        sensor_def = {'sensor_name': 'potential_temperature',
                      'attrs': {'ancillary_variables': 'salinity,temperature,pressure',
                                'observation_type': 'calculated'}}
        dba['data'] = np.append(dba['data'], np.expand_dims(p_temp, 1), axis=1)
        dba['sensors'].append(sensor_def)

        # if 'sound_speed' in ncw.nc_sensor_defs:
            # Calculate sound speed
        svel = calculate_sound_speed(t, p, salt, lat, lon)
        sensor_def = {'sensor_name': 'sound_speed',
                      'attrs': {
                          'ancillary_variables': 'conductivity,temperature,presssure,latitude,longitude',
                          'observation_type': 'calculated'}}
        dba['data'] = np.append(dba['data'], np.expand_dims(svel, 1), axis=1)
        dba['sensors'].append(sensor_def)

        # sci_water_*2 sensors for another CTD calculation
        ctd_sensors = []
        found_ctd_sensors = [s for s in ALT_CTD_SENSORS if s in dba_sensors]
        if len(found_ctd_sensors) == len(ALT_CTD_SENSORS):
            ctd_sensors = ALT_CTD_SENSORS

        if not ctd_sensors:
            logging.warning('Alternate CTD sensors not found in {:s}'.format(dba['file_metadata']['source_file']))
            return dba
        else:
            lati = dba_sensors.index(ctd_sensors[0])
            loni = dba_sensors.index(ctd_sensors[1])
            pi = dba_sensors.index(ctd_sensors[2])
            ti = dba_sensors.index(ctd_sensors[3])
            ci = dba_sensors.index(ctd_sensors[4])

            lat = dba['data'][:, lati]
            lon = dba['data'][:, loni]
            # convert pressure from bar to db
            p = dba['data'][:, pi] * 10
            t = dba['data'][:, ti]
            c = dba['data'][:, ci]

            # Calculate salinity
            salt = calculate_practical_salinity(c, t, p)
            # Calculate density
            density = calculate_density(t, p, salt, lat, lon)
            # Calculate potential temperature
            p_temp = pt0_from_t(salt, t, p)

            # Add parameters as long as they exist in ncw.nc_sensor_defs
            # if 'salinity' in ncw.nc_sensor_defs:
            sensor_def = {'sensor_name': 'salinity2',
                          'attrs': {'ancillary_variables': 'conductivity,temperature,presssure',
                                    'observation_type': 'calculated'}}
            dba['data'] = np.append(dba['data'], np.expand_dims(salt, 1), axis=1)
            dba['sensors'].append(sensor_def)

            # if 'density' in ncw.nc_sensor_defs:
            sensor_def = {'sensor_name': 'density2',
                          'attrs': {
                              'ancillary_variables': 'conductivity,temperature,presssure,latitude,longitude',
                              'observation_type': 'calculated'}}
            dba['data'] = np.append(dba['data'], np.expand_dims(density, 1), axis=1)
            dba['sensors'].append(sensor_def)

            # if 'potential_temperature' in ncw.nc_sensor_defs:
            sensor_def = {'sensor_name': 'potential_temperature2',
                          'attrs': {'ancillary_variables': 'salinity,temperature,pressure',
                                    'observation_type': 'calculated'}}
            dba['data'] = np.append(dba['data'], np.expand_dims(p_temp, 1), axis=1)
            dba['sensors'].append(sensor_def)

            # if 'sound_speed' in ncw.nc_sensor_defs:
            # Calculate sound speed
            svel = calculate_sound_speed(t, p, salt, lat, lon)
            sensor_def = {'sensor_name': 'sound_speed2',
                          'attrs': {
                              'ancillary_variables': 'conductivity,temperature,presssure,latitude,longitude',
                              'observation_type': 'calculated'}}
            dba['data'] = np.append(dba['data'], np.expand_dims(svel, 1), axis=1)
            dba['sensors'].append(sensor_def)

    return dba
