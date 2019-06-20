import os
from gncutils.writers.BaseTrajectoryNetCDFWriter import BaseTrajectoryNetCDFWriter
import numpy as np


class BaseProfileNetCDFWriter(BaseTrajectoryNetCDFWriter):

    # We want to inherit from TrajectoryNetCDFWriter but also add a few more properties,
    # specific to writing profile NetCDFs, so we have to call 
    # TrajectoryNetCDFWriter.__init__(self, ...)
    def __init__(self, config_path, nc_format='NETCDF4_CLASSIC', comp_level=1, clobber=False, profile_id=-1):

        BaseTrajectoryNetCDFWriter.__init__(self, config_path, nc_format=nc_format, comp_level=comp_level, clobber=clobber)
        self._profile_id = profile_id

        # Clear self._nc_sensor_defs to remove the trajectory sensor defs
        self._nc_sensor_defs = {}

        # Deployment specific sensor definitions configuration path
        # self._sensor_defs_config_path = os.path.join(config_path, 'profile-sensor_defs.json')
        # if not os.path.isfile(self._sensor_defs_config_path):
        #     raise FileNotFoundError('Sensor definitions file not found: {:s}'.format(self._sensor_defs_config_path))

        # Need to reset the config_path to trigger reading of profile-based sensor definitions from self._sensor_defs_
        # config_path
        self.config_path = config_path

        # cdm_data_type for global variables cdm_data_type and featureType
        self._cdm_data_type = 'Profile'

    @property
    def profile_id(self):
        return self._profile_id

    @profile_id.setter
    def profile_id(self, profile_id):
        self._profile_id = profile_id

    def set_profile_var(self):
        """ Sets Profile ID in NetCDF File
        """

        self.set_scalar('profile_id', self._profile_id)

        self._profile_id += 1

    def finish_nc(self):
        """Close the NetCDF file permanently and delete instance properties preventing
        new NetCDF files from being created
        """

        if not self._nc:
            self._logger.error('The NetCDF file has not been initialized')
            return

        if not self._nc.isopen():
            self._logger.warning('The NetCDF file is already closed: {:s}'.format(self._out_nc))
            return

        # Set profile variables
        self._update_profile_vars()
        # Update global geospatial attributes
        self._update_geospatial_global_attributes()
        # Update global time_coverage attributes
        self._update_time_coverage_global_attributes()

        self._nc.close()

        self._nc = None

        return self._out_nc

    def _update_profile_vars(self):
        """ Internal function that updates all profile variables
        before closing a file
        """

        self._logger.debug('Updating profile scalar variables')

        # Set the profile_id variable
        self.set_profile_var()

        time_sensor_def = self.sensor_def_exists('llat_time')
        if not time_sensor_def:
            self._logger.warning('Skipping creation of profile_time variable')
        else:
            time_var_name = time_sensor_def['nc_var_name']
            if time_var_name in self._nc.variables:
                self.set_scalar('profile_time', self._nc.variables[time_var_name][:].mean())
            else:
                self._logger.warning('Cannot set profile_time (missing {:s} variable)'.format(time_var_name))

        # Longitude sensor definition
        lon_sensor_def = self.sensor_def_exists('llat_longitude')
        # depth-average current longitude sensor definition
        lon_uv_sensor_def = self.sensor_def_exists('lon_uv')
        # Latitude sensor definition
        lat_sensor_def = self.sensor_def_exists('llat_latitude')
        # depth-averaged current latitude sensor definition
        lat_uv_sensor_def = self.sensor_def_exists('lat_uv')
        if not lon_sensor_def:
            self._logger.warning('Skipping creation of profile_lon')
        else:
            lon_var_name = lon_sensor_def['nc_var_name']
            if lon_var_name in self._nc.variables:
                mean_lon = np.nanmean(self._nc.variables[lon_var_name][:])
                self.set_scalar('profile_lon', mean_lon)
                if lon_uv_sensor_def:
                    self.set_scalar('lon_uv', mean_lon)
                else:
                    self._logger.debug('lon_uv not created: sensor definition does not exist')
            else:
                self._logger.warning('Cannot set profile_lon (missing {:s} variable)'.format(lon_var_name))

        if not lat_sensor_def:
            self._logger.warning('Skipping creation of profile_lat')
        else:
            lat_var_name = lat_sensor_def['nc_var_name']
            if lat_var_name in self._nc.variables:
                mean_lat = np.nanmean(self._nc.variables[lat_var_name][:])
                self.set_scalar('profile_lat', mean_lat)
                if lat_uv_sensor_def:
                    self.set_scalar('lat_uv', mean_lat)
                else:
                    self._logger.debug('lat_uv not created: sensor definition does not exist')
            else:
                self._logger.warning('Cannot set profile_lat (missing {:s} variable)'.format(lat_var_name))

    def __repr__(self):

        return '<ProfileNetCDFWriter(config_path={:s}, trajectory={:s}, format={:s})>'.format(self._config_path,
                                                                                              self._trajectory,
                                                                                              self._nc_format)
