#!/usr/bin/env python

"""Methods for filtering the array of profile start/stop times returned by 
gutils.yo.find_yo_extrema
"""

import numpy as np

from gncutils.yo.constants import (
    TIME_DIM,
    DATA_DIM
)

def default_profiles_filter(yo, profile_times):
    
    profile_times = filter_profiles_min_points(yo, profile_times)
    profile_times = filter_profiles_min_depthspan(yo, profile_times)
    profile_times = filter_profiles_min_timespan(yo, profile_times)
    
    return profile_times
    
def filter_profile_breaks(yo, profile_times):
    
    filt_p_times = np.empty((0,2))

    for p in profile_times:
    
        # Create the profile by finding all timestamps in yo that are included in the
        # window p
        pro = yo[np.logical_and(yo[:,0] >= p[0], yo[:,0] <= p[1])]
        
        pro = pro[np.all(~np.isnan(pro),axis=1)]
        
        # Diff the timestamps
        tdiff = np.diff(pro[:,0])
        # Median and stdev
        med = np.median(tdiff)
        std = np.std(tdiff)
        
        # Find profile time breaks
        p_breaks = np.where(tdiff > med+std)
        if not len(p_breaks):
            filt_p_times = np.append(filt_p_times, p)
            continue
            
        p0 = np.append(np.array(0), p_breaks[0].copy())
        p1 = np.append(p_breaks[0].copy()+1, pro.shape[0]-1)
        p_inds = np.column_stack((p0, p1))
        
        for i in p_inds:
            t0 = pro[i[0],0]
            t1 = pro[i[1],0]
            filt_p_times = np.append(filt_p_times, [[t0, t1]], axis=0)
            
    return filt_p_times
        
    
def filter_profiles_min_points(yo, profile_times, minpoints=3):
    """Returns profile start/stop times for which the indexed profile contains
    at least minpoints number of non-Nan points.
    
    Parameters:
        yo: Nx2 numpy array containing the timestamp and depth records
        profile_times: Nx2 numpy array containing the start/stop times of indexed
            profiles from gutils.yo.find_yo_extrema
            
    Options:
        minpoints: minimum number of points an indexed profile must contain to be
            consided valid <Default=2>
            
    Returns:
        Nx2 numpy array containing valid profile start/stop times
    """
    
    new_profile_times = np.full((0,2), np.nan)
    
    for p in profile_times:
        
        # Create the profile by finding all timestamps in yo that are included in the
        # window p
        pro = yo[np.logical_and(yo[:,TIME_DIM] >= p[0], yo[:,TIME_DIM] <= p[1])]
        
        # Eliminate NaN rows
        pro = pro[np.all(~np.isnan(pro),axis=1)]
        
        if pro.shape[0] >= minpoints:
            new_profile_times = np.append(new_profile_times, [p], axis=0)
            
    return new_profile_times
    
def filter_profiles_min_depthspan(yo, profile_times, mindepthspan=1):
    """Returns profile start/stop times for which the indexed profile depth range
    is at least mindepthspan.
    
    Parameters:
        yo: Nx2 numpy array containing the timestamp and depth records
        profile_times: Nx2 numpy array containing the start/stop times of indexed
            profiles from gutils.yo.find_yo_extrema
            
    Options:
        mindepthspan: minimum depth range (meters, decibars, bars) an indexed 
            profile must span to be considered valid <Default=1>
            
    Returns:
        Nx2 numpy array containing valid profile start/stop times
    """
    
    new_profile_times = np.full((0,2), np.nan)
    
    for p in profile_times:
        
        pro = yo[np.logical_and(yo[:,TIME_DIM] >= p[0], yo[:,TIME_DIM] <= p[1])]
        
        # Eliminate NaN rows
        pro = pro[np.all(~np.isnan(pro),axis=1)]
        
        if np.max(pro[:,DATA_DIM]) - np.min(pro[:,DATA_DIM]) >= mindepthspan:
            new_profile_times = np.append(new_profile_times, [p], axis=0)
            
    return new_profile_times
            
def filter_profiles_min_timespan(yo, profile_times, mintimespan=10):
    """Returns profile start/stop times for which the indexed profile spans at 
    least mintimespan seconds.
    
    Parameters:
        yo: Nx2 numpy array containing the timestamp and depth records
        profile_times: Nx2 numpy array containing the start/stop times of indexed
            profiles from gutils.yo.find_yo_extrema
            
    Options:
        mintimespan: minimum number of seconds an indexed profile must span to be
            considered valid <Default=10>
            
    Returns:
        Nx2 numpy array containing valid profile start/stop times
    """
    
    new_profile_times = np.full((0,2), np.nan)
    
    for p in profile_times:
        
        pro = yo[np.logical_and(yo[:,TIME_DIM] >= p[0], yo[:,TIME_DIM] <= p[1])]
        
        # Eliminate NaN rows
        pro = pro[np.all(~np.isnan(pro),axis=1)]
        
        if np.max(pro[:,TIME_DIM]) - np.min(pro[:,TIME_DIM]) >= mintimespan:
            new_profile_times = np.append(new_profile_times, [p], axis=0)
            
    return new_profile_times
