import os
from gncutils.TrajectoryNetCDFWriter import TrajectoryNetCDFWriter


class ProfileNetCDFWriter(TrajectoryNetCDFWriter):

    # We want to inherit from TrajectoryNetCDFWriter but also add a few more properties,
    # specific to writing profile NetCDFs, so we have to call 
    # TrajectoryNetCDFWriter.__init__(self, ...)
    def __init__(self, config_path, nc_format='NETCDF4_CLASSIC', comp_level=1, clobber=False, profile_id=1):

        TrajectoryNetCDFWriter.__init__(self, config_path, nc_format=nc_format, comp_level=comp_level, clobber=clobber)
        self._profile_id = profile_id

        # Clear self._nc_sensor_defs to remove the trajectory sensor defs
        self._nc_sensor_defs = {}

        # Deployment specific sensor definitions configuration path
        self._sensor_defs_config_path = os.path.join(config_path, 'profile-sensor_defs.json')
        if not os.path.isfile(self._sensor_defs_config_path):
            raise FileNotFoundError('Sensor definitions file not found: {:s}'.format(self._sensor_defs_config_path))

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

        # Set the profile_id variable
        self.set_profile_var()

        time_var_name = self.sensor_def_exists('llat_time')
        if not time_var_name:
            self._logger.warning('Skipping creation of profile_time variable')
        else:
            if time_var_name in self._nc.variables:
                self.set_scalar('profile_time', self._nc.variables[time_var_name][:].mean())
            else:
                self._logger.warning('Cannot set profile_time (missing {:s} variable)'.format(time_var_name))

        lat_var_name = self.sensor_def_exists('llat_latitude')
        lon_var_name = self.sensor_def_exists('llat_longitude')
        if lon_var_name in self._nc.variables:
            self.set_scalar('profile_lon', self._nc.variables[lon_var_name][:].mean())
        else:
            self._logger.warning('Cannot set profile_lon (missing {:s} variable)'.format(lon_var_name))

        if lat_var_name in self._nc.variables:
            self.set_scalar('profile_lat', self._nc.variables[lat_var_name][:].mean())
        else:
            self._logger.warning('Cannot set profile_lat (missing {:s} variable)'.format(lat_var_name))

    def __repr__(self):

        return '<ProfileNetCDFWriter(config_path={:s}, trajectory={:s}, format={:s})>'.format(self._config_path,
                                                                                              self._trajectory,
                                                                                              self._format)
