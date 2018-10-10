#!/usr/bin/env python

import numpy as np
import logging
import os
from netCDF4 import Dataset
from gncutils.yo.constants import TIME_DIM, DATA_DIM
from gncutils import clean_dataset, boxcar_smooth_dataset
from gncutils.yo.filters import default_profiles_filter

logger = logging.getLogger(os.path.basename(__file__))


def find_profiles(yo):
    """ Construct the depth time-series from a parsed dba['data'] array and find
    the profile minima and maxima.  The resulting indexed profiles are filtered
    using gnc.yo.filters.default_profiles_filter which filters on defaults for the
    min number of points, min depth span and min time span.
    
    Arguments
    data: an array of dicts returned from create_llat_dba_reader containing the
        individual rows of a parsed dba file
        
    Returns
    profile_times: a 2 column array containing the start and stop times of each
        indexed profile.  
    """

    profile_times = find_yo_extrema(yo[:, TIME_DIM], yo[:, DATA_DIM])
    if len(profile_times) == 0:
        return profile_times

    filter_profile_times = default_profiles_filter(yo, profile_times)

    return filter_profile_times


def build_yo(glider_data):
    """Return the yo (time-depth series) from dba_reader['data']."""
    sensor_names = [s['sensor_name'] for s in glider_data['sensors']]

    # Find llat_time and llat_pressure
    if 'llat_time' not in sensor_names:
        logger.warning('Parsed dba does not contain llat_time: {:s}'.format(glider_data['file_metadata']['source_file']))
        return
    if 'llat_pressure' not in sensor_names:
        logger.warning(
            'Parsed dba does not contain llat_pressure: {:s}'.format(glider_data['file_metadata']['source_file']))
        return

    ti = sensor_names.index('llat_time')
    pi = sensor_names.index('llat_pressure')

    return glider_data['data'][:, [ti, pi]]


def slice_sensor_data(glider_data, sensors=[]):
    """Return the sensor subsetted data array from dba_reader['data'] containing the sensors specified in sensors.  If
    no sensors are specified, the yo (time-depth series) is returned."""
    if not sensors:
        return build_yo(glider_data)

    sensor_names = [s['sensor_name'] for s in glider_data['sensors']]

    num_rows = glider_data['data'].shape[0]
    sensor_data = np.empty((num_rows, len(sensors))) * np.nan
    for s in list(range(0, len(sensors))):
        sensor = sensors[s]
        if sensor not in sensor_names:
            logger.warning('{:s} not found in glider data structure'.format(sensor))
            continue

        col = sensor_names.index(sensor)
        sensor_data[:, s] = glider_data['data'][:, col]

    return sensor_data


def binarize_diff(data):
    data[data <= 0] = -1
    data[data >= 0] = 1
    return data


def calculate_delta_depth(interp_data):
    delta_depth = np.diff(interp_data)
    delta_depth = binarize_diff(delta_depth)

    return delta_depth


def create_profile_entry(dataset, start, end):
    time_start = dataset[start, TIME_DIM]
    time_end = dataset[end - 1, TIME_DIM]
    depth_start = dataset[start, DATA_DIM]
    depth_end = dataset[end - 1, DATA_DIM]
    return {
        'index_bounds': (start, end),
        'time_bounds': (time_start, time_end),
        'depth_bounds': (depth_start, depth_end)
    }


def find_yo_extrema(timestamps, depth, tsint=10):
    """Returns the start and stop timestamps for every profile indexed from the 
    depth timeseries

    Parameters:
        time, depth

    Returns:
        A Nx2 array of the start and stop timestamps indexed from the yo

    Use filter_yo_extrema to remove invalid/incomplete profiles
    """

    # Create Nx2 numpy array of profile start/stop times - kerfoot method
    profile_times = np.empty((0, 2))

    # validate_glider_args(timestamps, depth)

    est_data = np.column_stack((
        timestamps,
        depth
    ))

    # Set negative depth values to NaN
    est_data[np.any(est_data <= 0, axis=1), :] = float('nan')

    # Remove NaN rows
    est_data = clean_dataset(est_data)
    if len(est_data) < 2:
        logger.debug('Skipping yo that contains < 2 rows')
        return np.empty((0, 2))

    # Create the fixed timestamp array from the min timestamp to the max timestamp
    # spaced by tsint intervals
    min_ts = est_data[:, 0].min()
    max_ts = est_data[:, 0].max()
    if max_ts - min_ts < tsint:
        logger.warning('Not enough timestamps for yo interpolation')
        return np.empty((0, 2))
    
    ts = np.arange(min_ts, max_ts, tsint)
    # Stretch estimated values for interpolation to span entire dataset
    interp_z = np.interp(
        ts,
        est_data[:, 0],
        est_data[:, 1],
        left=est_data[0, 1],
        right=est_data[-1, 1]
    )

    filtered_z = boxcar_smooth_dataset(interp_z, int(tsint / 2))

    delta_depth = calculate_delta_depth(filtered_z)

    # interp_indices = np.argwhere(delta_depth == 0).flatten()

    p_inds = np.empty((0, 2))
    inflections = np.where(np.diff(delta_depth) != 0)[0]
    if not inflections.any():
        return profile_times

    p_inds = np.append(p_inds, [[0, inflections[0]]], axis=0)
    for p in range(len(inflections) - 1):
        p_inds = np.append(p_inds, [[inflections[p], inflections[p + 1]]], axis=0)
    p_inds = np.append(p_inds, [[inflections[-1], len(ts) - 1]], axis=0)

    # profile_timestamps = np.empty((0,2))
    ts_window = tsint * 2

    # Create orig GUTILS return value - lindemuth method
    # Initialize an nx3 numpy array of nans
    profiled_dataset = np.full((len(timestamps), 3), np.nan)
    # Replace TIME_DIM column with the original timestamps
    profiled_dataset[:, TIME_DIM] = timestamps
    # Replace DATA_DIM column with the original depths
    profiled_dataset[:, DATA_DIM] = depth

    # # Create Nx2 numpy array of profile start/stop times - kerfoot method
    profile_times = np.full((p_inds.shape[0], 2), np.nan)

    # Start profile index
    profile_ind = 0
    # Iterate through the profile start/stop indices
    for p in p_inds:
        # Profile start row
        p0 = int(p[0])
        # Profile end row
        p1 = int(p[1])
        # Find all rows in the original yo that fall between the interpolated timestamps
        profile_i = np.flatnonzero(np.logical_and(profiled_dataset[:, TIME_DIM] >= ts[p0] - ts_window,
                                                  profiled_dataset[:, TIME_DIM] <= ts[p1] + ts_window))
        # Slice out the profile
        pro = profiled_dataset[profile_i]
        # Find the row index corresponding to the minimum depth
        try:
            min_i = np.nanargmin(pro[:, 1])
        except ValueError as e:
            logger.warning(e)
            continue
        # Find the row index corresponding to the maximum depth
        try:
            max_i = np.nanargmax(pro[:, 1])
        except ValueError as e:
            logger.warning(e)
            continue
        # Sort the min/max indices in ascending order
        sorted_i = np.sort([min_i, max_i])
        # Set the profile index 
        profiled_dataset[profile_i[sorted_i[0]]:profile_i[sorted_i[1]], 2] = profile_ind

        # kerfoot method
        profile_times[profile_ind, :] = [timestamps[profile_i[sorted_i[0]]], timestamps[profile_i[sorted_i[1]]]]
        # Increment the profile index
        profile_ind += 1

        # profile_timestamps = np.append(profile_timestamps, [[est_data[profile_i[0][0],0], est_data[profile_i[0][-1],0]]], axis=0)

    # return profiled_dataset
    return profile_times


def find_navoceano_yo_extrema(nc_file):

    if not os.path.isfile(nc_file):
        logger.error('Invalid NetCDF file specified: {:s}'.format(nc_file))
        return

    try:
        nci = Dataset(nc_file, 'r')
    except IOError as e:
        logger.error('Error opening NetCDF file {:s}: {:}'.format(nc_file, e))
        return

    if 'prof_start_index' not in nci.variables:
        logger.warning('Missing prof_start_index: {:s}'.format(nc_file))
        return
    if 'prof_end_index' not in nci.variables:
        logger.warning('Missing prof_end_index: {:s}'.format(nc_file))
        return
    if 'time' not in nci.variables:
        logger.warning('Missing time: {:s}'.format(nc_file))
        return

    # Grab the prof_start_index
    start_p_inds = nci.variables['prof_start_index'][:]
    # Grab the prof_end_index
    end_p_inds = nci.variables['prof_end_index'][:]

    # Create the profile index array
    p_inds = np.column_stack((start_p_inds, end_p_inds))

    # Grab time
    time = nci.variables['time'][:].data

    # Create Nx2 numpy array of profile start/stop times - kerfoot method
    profile_times = np.full((p_inds.shape[0], 2), np.nan)
    profile_ind = 0
    # Find the corresponding timestamps for p_inds
    for t0,t1 in p_inds:
        profile_times[profile_ind,:] = [time[t0], time[t1]]
        profile_ind += 1

