
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from netCDF4 import num2date

_days_formatter = mdates.DayLocator()
_hours_formatter = mdates.HourLocator()
_days_format = mdates.DateFormatter('%m/%d')
_hours_format = mdates.DateFormatter('%H:%M')


def plot_yo(yo, profile_times):
    """Plot the glider yo and the indexed profiles"""

    yo = yo[~np.isnan(yo).any(axis=1)]

    ax = plt.gca()

    ax.xaxis.set_major_locator(_days_formatter)
    ax.xaxis.set_major_formatter(_days_format)
    ax.xaxis.set_minor_locator(_hours_formatter)
    ax.xaxis.set_minor_formatter(_hours_format)

    z = yo[:, 1]
    t = num2date(yo[:, 0], 'seconds since 1970-01-01 00:00:00Z')
    # yo[:,0] = num2date(yo[:,0], 'seconds since 1970-01-01 00:00:00Z')
    plt.plot(t, -yo[:,1], 'k.', axes=ax)

    for p in profile_times:

        # Create the profile by finding all timestamps in yo that are included in the
        # window p
        p_inds = np.where(np.logical_and(yo[:,0] >= p[0], yo[:,0] <= p[1]))
        p_times = t[p_inds]
        p_data = z[p_inds]
        # pro = yo[np.logical_and(t >= p[0], t <= p[1])]
        #
        # pro = pro[np.all(~np.isnan(pro),axis=1)]

        plt.plot(p_times, -p_data)

    plt.show()


def plot_sensor_timeseries(glider_data, sensor):

    sensor_names = [s['sensor_name'] for s in glider_data['sensors']]

    if sensor not in sensor_names:
        return

    si = sensor_names.index(sensor)
    ti = sensor_names.index('llat_time')

    t = num2date(glider_data['data'][:, ti], glider_data['sensors'][ti]['attrs']['units'])
    data = glider_data['data'][:, si]

    ax = plt.gca()
    ax.xaxis.set_major_locator(_days_formatter)
    ax.xaxis.set_major_formatter(_days_format)
    ax.xaxis.set_minor_locator(_hours_formatter)
    ax.xaxis.set_minor_formatter(_hours_format)
    plt.plot(t, data, 'k.', axes=ax)

    plt.show()
