import os
import logging
import json
import datetime
from copy import deepcopy
import numpy as np
import uuid
from netCDF4 import Dataset, stringtoarr
from gncutils.constants import NC_FILL_VALUES
from shapely.geometry import Polygon
from dateutil import parser
from gncutils.constants import NETCDF_FORMATS, NC_FILL_VALUES, REQUIRED_SENSOR_DEFS_KEYS, CF_VARIABLE_ATTRIBUTES


class BaseTrajectoryNetCDFWriter(object):

    def __init__(self, config_path, nc_format='NETCDF4_CLASSIC', comp_level=1, clobber=False):
        """Create instance of the TrajectoryNetCDFWriter:
            1. Set and validate default configuration paths
            2. Load default sensor definitions
            3. Load default global attributes
            4. Set deployment specific configurations
        
        Does not create NetCDF files or write data to NetCDF files.  See init_nc, 
        open_nc and finish_nc for file creation and write operations.
        
        Parameters:
            config_path: location of deployment specific configuration files
            
        Options:
            format: Valid python netCDF4 package NetCDF file type
            comp_level: Compression level. Ignored if compression not supported
                for specified format.
            clobber: True overwrites existing files.
        """

        # Create logger
        self._logger = logging.getLogger(os.path.basename(__file__))

        if not os.path.isdir(config_path):
            raise FileNotFoundError('Invalid configuration path: {:s}'.format(config_path))

        # Deployment-specific configuration file path containing:
        #   global_attributes.json
        #   instruments.json
        #   deployment.json
        #   sensor_defs.json [optional]
        self._config_path = None
        # Trajectory string corresponding to the start date of the deployments
        self._trajectory = None
        # CDM data type
        self._cdm_data_type = 'Trajectory'
        # Deployment parameters set in deployment.json
        self._deployment_configs = None
        # Set the NetCDF file type format
        self._nc_format = nc_format
        # Set the NetCDF4 compression level
        self._comp_level = comp_level
        # Set to true to clobber existing files, false to skip existing NetCDF files
        self._clobber = clobber

        # Deployment specific configuration path
        self._deployment_config_path = os.path.join(config_path, 'deployment.json')
        if not os.path.isfile(self._deployment_config_path):
            raise FileNotFoundError(
                'Deployment configuration file not found: {:s}'.format(self._deployment_config_path))
        # Deployment specific global attributes configuration path
        self._global_attributes_path = os.path.join(config_path, 'global_attributes.json')
        if not os.path.isfile(self._global_attributes_path):
            raise FileNotFoundError(
                'Deployment global attributes file not found: {:s}'.format(self._global_attributes_path))
        # Deployment specific instruments configuration path
        self._instruments_config_path = os.path.join(config_path, 'instruments.json')
        if not os.path.isfile(self._instruments_config_path):
            raise FileNotFoundError(
                'Deployment instruments configuration file not found: {:s}'.format(self._instruments_config_path))
        # Deployment specific sensor definitions configuration path
        self._sensor_defs_config_path = os.path.join(config_path, 'sensor_defs.json')
        if not os.path.isfile(self._sensor_defs_config_path):
            raise FileNotFoundError('Sensor definitions file not found: {:s}'.format(self._sensor_defs_config_path))

        # Sensor defintions from self._default_sensor_defs_path
        self._default_sensor_defs = {}
        # Sensor definitions from self._sensor_defs_config_path
        self._config_sensor_defs = {}
        # self._default_sensor_defs updated with self._config_sensor_defs and
        # reader['sensors'].  Maps raw glider sensor names to NetCDF variable names
        self._nc_sensor_defs = {}

        # Stores self._deployment_config_path, self._global_attributes_path and
        # self._instruments_config_path file contents
        self._attributes = {'deployment': {},
                            'global': {},
                            'instruments': {}}
        # Python NetCDF4 Dataset instance
        self._nc = None
        # Unlimited record dimension name
        self._record_dimension = None

        # Set the deployment configuration path and configure self
        self.config_path = config_path
        # Deployment configuration parameter access
        self._deployment_configs = self._attributes['deployment']

        # Required variable definitions
        self._llat_vars = ['llat_time',
                           'llat_depth',
                           'llat_latitude',
                           'llat_longitude']

        # Output NetCDF file name
        self._out_nc = None

    @property
    def deployment_config_file(self):
        return self._deployment_config_path

    @property
    def global_attributes_file(self):
        return self._global_attributes_path

    @property
    def instruments_config_file(self):
        return self._instruments_config_path

    @property
    def sensor_defs_file(self):
        return self._sensor_defs_config_path

    @property
    def nc(self):
        return self._nc

    @property
    def deployment_configs(self):
        return self._deployment_configs

    @property
    def config_path(self):

        return self._config_path

    @config_path.setter
    def config_path(self, config_path):
        """Configure the TrajectoryNetCDFWriter instance with deployment specific
        settings:
        
        1. Set the deployment configuration path
        2. Update default sensor definitions with the deployment specific sensor 
            definitions.
        3. Update default global attributes with the deployment specific
            deployment attributes
        4. Load glider instrument information.
        """

        # Do now allow changing of the configuration path if self._nc is not None
        if self._nc:
            self._logger.error('Cannot reconfigure with existing netCDF4.Dataset: {:s}'.format(self._nc))
            return

        if not os.path.isdir(config_path):
            raise FileNotFoundError('Invalid configuration path: {:s}'.format(config_path))

        # Set the configuration path    
        self._config_path = config_path

        # Load the configuration path sensor definitions
        self._load_config_sensor_defs()
        if not self._config_sensor_defs:
            raise self.GliderNetCDFWriterError(
                'Failed to load configuration sensor definitions: {:s}'.format(self._sensor_defs_config_path))

        # Update self._default_sensor_defs with self._config_sensor_defs
        self._update_sensor_defs()

        # Read in deployment configuration and NetCDF attribute files
        config_status = self._read_configs()
        if not config_status:
            raise self.GliderNetCDFWriterError(
                'Failed to configure writer with one or more configuration files: {:s}'.format(self._config_path))

        # Create the trajectory name.  Use the trajectory_name, if present, in deployment.json.
        # Otherwise, parse the trajectory_datetime in deployment.json
        if 'trajectory_name' in self._attributes['deployment'] and len(
                self._attributes['deployment']['trajectory_name'].strip()) > 0:
            self._trajectory = self._attributes['deployment']['trajectory_name']
        else:
            if 'trajectory_datetime' not in self._attributes['deployment']:
                self._logger.error(
                    'No trajectory_datetime key in deployment.json: {:s}'.format(self._deployment_config_path))
                return
            try:
                trajectory_dt = parser.parse(self._attributes['deployment']['trajectory_datetime'])
                self._trajectory = '{:s}-{:s}-rt'.format(self.attributes['deployment']['glider'],
                                                         trajectory_dt.strftime('%Y%m%dT%H%M'))
            except ValueError as e:
                self._logger.error('Error parsing deployment trajectory_date: {:s} ({:s})'.format(
                    self._attributes['deployment']['trajectory_date'], e))
                return

    @property
    def trajectory(self):
        return self._trajectory

    @trajectory.setter
    def trajectory(self, traj_string):
        if not traj_string or type(traj_string) != str:
            raise ValueError('Trajectory must be a string')

        self._trajectory = traj_string

    @property
    def nc_format(self):

        return self._nc_format

    @nc_format.setter
    def nc_format(self, nc_format):

        if format not in NETCDF_FORMATS:
            raise ValueError('Invalid NetCDF format: {:s}'.format(format))

        self._nc_format = nc_format

    @property
    def comp_level(self):

        return self._comp_level

    @comp_level.setter
    def comp_level(self, comp_level):

        if comp_level not in range(11):
            raise ValueError('Compression level must be a value from 0 - 10')

        self._comp_level = comp_level

    @property
    def clobber(self):

        return self._clobber

    @clobber.setter
    def clobber(self, clobber):

        if type(clobber) != bool:
            raise ValueError('Clobber value must be boolean')

        self._clobber = clobber

    @property
    def default_sensor_defs(self):

        return self._default_sensor_defs

    @property
    def config_sensor_defs(self):

        return self._config_sensor_defs

    @property
    def nc_sensor_defs(self):

        return self._nc_sensor_defs

    @property
    def config_sensor_defs(self):

        return self._config_sensor_defs

    @property
    def attributes(self):

        return self._attributes

    @property
    def llat_vars(self):

        return self._llat_vars

    @classmethod
    def validate_sensor_defs(self, sensor_defs):

        if not sensor_defs:
            self._logger.error('No sensor definitions specified')
            return

        status = 0
        for sensor, sensor_def in sensor_defs.items():

            for required_key in REQUIRED_SENSOR_DEFS_KEYS:
                if required_key not in sensor_def:
                    logging.error('Missing required key in {:s} sensor definition: {:s}'.format(sensor, required_key))
                    status = 1

            if 'attrs' not in sensor_def:
                self._logger.warning('No attributes specified for {:s} sensor definition'.format(sensor))
                status = 1
                continue

            elif not sensor_def['attrs']:
                logging.warning('{:s} sensor definition does not contain any attributes'.format(sensor))
                status = 1
                continue

            for cf_att in CF_VARIABLE_ATTRIBUTES:
                if cf_att not in sensor_def['attrs']:
                    logging.warning('{:s} sensor definition missing CF attribute: {:s}'.format(sensor, cf_att))
                    status = 1

            if 'type' in sensor_def:
                if sensor_def['type'] not in NC_FILL_VALUES:
                    logging.error('Invalid NetCDF variable datatype for {:s}: {:s}'.format(sensor, sensor_def['type']))
                    if sensor_def['type'].startswith('<'):
                        suggested_type = sensor_def['type'].replace('<', '')
                        if suggested_type in NC_FILL_VALUES:
                            logging.info(
                                'Suggested NetCDF variable datatype for {:s}: {:s}'.format(sensor, suggested_type))
                    status = 1

        return status

    def set_title(self, title_string):
        """Set the NetCDF global title attribute to title_string"""
        self._nc.title = title_string

    def init_nc(self, out_nc):
        """Initialize a new NetCDF file (netCDF4.Dataset):
        
        1. Open the file in write mode
        2. Create the record dimension
        3. Set all global attributes
        4. Update the history global attribute
        5. Create the platform variable
        6. Create the instrument variable
        7. Close the file
        """

        if not self._record_dimension:
            self._logger.error('No record dimension found in sensor definitions')
            return

        if self._nc:
            self._logger.error('Existing netCDF4.Dataset: {}'.format(self._nc))
            return

        try:
            self._nc = Dataset(out_nc, mode='w', clobber=True, format=self._nc_format)
        except OSError as e:
            self._logger.critical('Error initializing {:s} ({})'.format(out_nc, e))
            return

        # Create the record dimension
        self._nc.createDimension(self._record_dimension['nc_var_name'], size=self._record_dimension['dimension_length'])

        # Store the NetCDF destination name
        self._out_nc = out_nc

        # Write global attributes
        # Add date_created, date_modified, date_issued globals
        nc_create_ts = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        self._attributes['global']['date_created'] = nc_create_ts
        self._attributes['global']['date_issued'] = nc_create_ts
        self._attributes['global']['date_modified'] = nc_create_ts
        # Add history attribute if not present in self._attributes['global']
        if 'history' not in self._attributes['global']:
            self._attributes['global']['history'] = ' '
        if 'id' not in self._attributes['global']:
            self._attributes['global']['id'] = ' '

        # Add the global cdm_data_type attribute
        # MUST be 'Trajectory'
        self._attributes['global']['cdm_data_type'] = self._cdm_data_type
        # Add the global featureType attribute
        # MUST be 'trajectory'
        self._attributes['global']['featureType'] = self._cdm_data_type.lower()

        # Write the NetCDF global attributes
        self.set_global_attributes()

        # Update global history attribute
        self.update_history('{:s} created'.format(out_nc))

        # Create platform container variable
        self.set_platform()

        # Create instrument container variables
        self.set_instruments()

        # Generate and add a UUID global attribute
        self._nc.setncattr('uuid', '{:s}'.format(str(uuid.uuid4())))

        self._nc.close()

    def open_nc(self):
        """Open the current NetCDF file (self._nc) in append mode and set the
        record dimension array index for appending data.
        """

        if not self._out_nc:
            self._logger.error('The NetCDF file has not been initialized')
            return

        if self._nc and self._nc.isopen():
            self._logger.error('netCDF4.Dataset is already open: {:s}'.format(self._nc))
            return
            # raise GliderNetCDFWriterException('netCDF4.Dataset is already open: {:s}'.format(self._nc))

        # Open the NetCDF in append mode
        self._nc = Dataset(self._out_nc, mode='a')

    def finish_nc(self):
        """Close the NetCDF file permanently, updates some global attributes and
        delete instance properties to prevent any further appending to the file.
        The file must be reopened with self.open_nc if further appending is 
        required.
        """

        if not self._out_nc or not os.path.isfile(self._out_nc):
            self._logger.error('No output NetCDF file specified')
            return

        if not self._nc:
            self._logger.error('The NetCDF file has not been initialized')
            return

        if not self._nc.isopen():
            self._logger.warning('The NetCDF file is already closed: {:s}'.format(self._out_nc))
            return

        # Update global geospatial attributes
        self._update_geospatial_global_attributes()
        # Update global time_coverage attributes
        self._update_time_coverage_global_attributes()

        # Update the UUID global attribute
        self._nc.setncattr('uuid', '{:s}'.format(str(uuid.uuid4())))

        self._nc.close()

        self._nc = None

        return True

    def update_history(self, message):
        """ Updates the global history attribute with the message appended to
        and ISO8601:2004 timestamp
        """

        # Get timestamp for this access
        now_time_ts = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        history_string = '{:s}: {:s}\n'.format(now_time_ts, message)
        if 'history' not in self._nc.ncattrs():
            self._nc.setncattr('history', history_string)
            return

        previous_history = self._nc.history.strip()
        if not previous_history:
            self._nc.history = history_string
        else:
            self._nc.history += history_string

    def set_container_variables(self):

        if not self._nc:
            self._logger.warning('NetCDF file must be initialized before adding container variables')
            return

        container_variables = [sensor for sensor in self.nc_sensor_defs.keys() if
                               'dimension' in self.nc_sensor_defs[sensor] and not self.nc_sensor_defs[sensor][
                                   'dimension']]
        for container_variable in container_variables:
            if 'nc_var_name' not in self._nc_sensor_defs[container_variable]:
                self._logger.warning(
                    '{:s} sensor definition does not contain an nc_var_name key'.format(container_variable))
                continue

            nc_var_name = self._nc_sensor_defs[container_variable]['nc_var_name']

            if nc_var_name in self._nc.variables:
                continue
            elif 'attrs' not in self.nc_sensor_defs[container_variable]:
                continue

            self.set_scalar(container_variable)
            for k, v in sorted(self.nc_sensor_defs[container_variable]['attrs'].items()):
                if k.lower() == '_fillvalue' or k.lower() == 'missing_value':
                    continue
                self._nc.variables[nc_var_name].setncattr(k, v)

    def set_platform(self):
        """ Creates a variable that describes the glider
        """

        self.set_scalar('platform')
        for key, value in sorted(self._attributes['deployment']['platform'].items()):
            self._nc.variables['platform'].setncattr(key, value)

    def set_global_attributes(self):
        """ Sets a dictionary of values as global attributes
        """

        for key, value in sorted(self._attributes['global'].items()):
            try:
                self._nc.setncattr(key, value)
            except TypeError as e:
                self._logger.error('Error setting global attribute {:s} ({:})'.format(key, e))

    def add_global_attribute(self, name, value, override=False):
        """Adds new global attributes contained in self._attributes['global'].  Set override to True to override
        existing global attributes. Must be called before calling self.init() or self.set_global_attriubtes"""

        if name in self._attributes['global'] and not override:
            self._logger.debug('Skipping existing global attribute: {:s}'.format(name))
            return

        self._attributes['global'][name] = value

    def _update_time_coverage_global_attributes(self):
        """Update all global time_coverage attributes.  The following global
        attributes are created/updated:
            time_coverage_start
            time_coverage_end
            time_coverage_duration
        """

        # time_var_name = self.sensor_def_exists('drv_timestamp')
        time_sensor_def = self.sensor_def_exists('llat_time')
        if not time_sensor_def:
            self._logger.warning('Failed to set global time_coverage_start/end attributes')
            return

        time_var_name = time_sensor_def['nc_var_name']
        min_timestamp = self._nc.variables[time_var_name][:].min()
        max_timestamp = self._nc.variables[time_var_name][:].max()
        try:
            dt0 = datetime.datetime.utcfromtimestamp(min_timestamp)
        except ValueError as e:
            self._logger.error('Error parsing min {:s}: {:s} ({:s})'.format(time_var_name, min_timestamp, e))
        try:
            dt1 = datetime.datetime.utcfromtimestamp(max_timestamp)
        except ValueError as e:
            self._logger.error('Error parsing max {:s}: {:s} ({:s})'.format(time_var_name, max_timestamp, e))

        self._nc.setncattr('time_coverage_start', dt0.strftime('%Y-%m-%dT%H:%M:%SZ'))
        self._nc.setncattr('time_coverage_end', dt1.strftime('%Y-%m-%dT%H:%M:%SZ'))
        self._nc.setncattr('time_coverage_duration', self.delta_to_iso_duration(dt1 - dt0))

        # Calculate the approximate time_coverage_resolution
        num_seconds = dt1 - dt0
        resolution_seconds = datetime.timedelta(0, int(
            num_seconds.total_seconds() / self._nc.variables[time_var_name].size))
        self._nc.setncattr('time_coverage_resolution', self.delta_to_iso_duration(resolution_seconds))

    def _update_geospatial_global_attributes(self):
        """Update all global geospatial_ min/max attributes.  The following global
        attributes are created/updated:
            geospatial_lat_min
            geospatial_lat_max
            geospatial_lon_min
            geospatial_lon_max
            geospatial_bounds
            geospatial_vertical_min
            geospatial_vertical_max
        """

        min_lat = " "
        max_lat = " "
        min_lon = " "
        max_lon = " "
        min_depth = " "
        max_depth = " "
        depth_resolution = " "
        polygon_wkt = u'POLYGON EMPTY'

        lon_sensor_def = self.sensor_def_exists('llat_longitude')
        lat_sensor_def = self.sensor_def_exists('llat_latitude')
        if not lon_sensor_def or not lat_sensor_def:
            self._logger.warning('Failed to set geospatial global attributes')
        else:

            lat_var_name = lat_sensor_def['nc_var_name']
            lon_var_name = lon_sensor_def['nc_var_name']

            if lat_var_name in self._nc.variables and lon_var_name in self._nc.variables:
                min_lat = self._nc.variables[lat_var_name][:].min()
                max_lat = self._nc.variables[lat_var_name][:].max()
                min_lon = self._nc.variables[lon_var_name][:].min()
                max_lon = self._nc.variables[lon_var_name][:].max()

                # Make sure we have non-Nan for all values
                if not np.any(np.isnan([min_lat, max_lat, min_lon, max_lon])):
                    # Create polygon WKT and set geospatial_bounds
                    coords = ((max_lat, min_lon),
                              (max_lat, max_lon),
                              (min_lat, max_lon),
                              (min_lat, min_lon),
                              (max_lat, min_lon))
                    polygon = Polygon(coords)
                    polygon_wkt = polygon.wkt

        # Set the global attributes
        self._nc.setncattr('geospatial_lat_min', min_lat)
        self._nc.setncattr('geospatial_lat_max', max_lat)
        self._nc.setncattr('geospatial_lon_min', min_lon)
        self._nc.setncattr('geospatial_lon_max', max_lon)
        self._nc.setncattr('geospatial_bounds', polygon_wkt)

        depth_sensor_def = self.sensor_def_exists('llat_depth')
        if not depth_sensor_def:
            self._logger.warning('Failed to set global geospatial_vertical attributes')
        else:
            depth_var_name = depth_sensor_def['nc_var_name']
            if depth_var_name in self._nc.variables:
                try:
                    min_depth = np.nanmin(self._nc.variables[depth_var_name][:].min())
                    max_depth = np.nanmax(self._nc.variables[depth_var_name][:].max())
                    depth_resolution = int((max_depth - min_depth) / self._nc.variables[depth_var_name].size)
                except (TypeError, ValueError) as e:
                    self._logger.warning('{:s}: {:}'.format(self._out_nc, e))
                    depth_resolution = np.nan

        self._nc.setncattr('geospatial_vertical_min', min_depth)
        self._nc.setncattr('geospatial_vertical_max', max_depth)
        self._nc.setncattr('geospatial_verical_resolution', depth_resolution)

    def set_scalar(self, sensor, value=None):
        """Create the NetCDF scalar variable specified in self._nc_sensor_defs[sensor]

        Parameters:
            sensor: native glider sensor name
            value: optional scalar value to store in the variable

        Returns:
            True if created, None if not created
        """

        sensor_def = self.check_datatype_exists(sensor)
        if not sensor_def:
            self._logger.warning('No sensor definition found for NetCDF scalar: {:s}'.format(sensor))
            return

        # Store the value in the scalar if there is one
        if value:
            self._nc.variables[sensor_def['nc_var_name']].assignValue(value)

        return True

    def check_datatype_exists(self, sensor):
        """Creates the NetCDF variable for sensor using the sensor definition in self._nc_sensor_defs[sensor] if the
        sensor definition exists

        Parameters:
            sensor: native glider sensor name

        Returns:
            sensor_def: the sensor definition from self._nc_sensor_defs[sensor]

            or

            None if the sensor definition does not exist or if there was an error creatingthe NetCDF variable
        """

        sensor_def = self.sensor_def_exists(sensor)
        if not sensor_def:
            return sensor_def

        var_created = None
        if sensor_def['nc_var_name'] in self._nc.variables:
            self._logger.debug('NetCDF variables {:s} already exists'.format(sensor_def['nc_var_name']))
            return sensor_def
        else:
            var_created = self.set_datatype(sensor)

        if not var_created:
            return var_created

        return sensor_def

    def sensor_def_exists(self, sensor):
        """Return the sensor definition for the specified sensor if it exists and is properly configured.  Returns None
        otherwise or if the sensor definition is missing any of REQUIRED_SENSOR_DEFS_KEYS"""

        if sensor not in self._nc_sensor_defs:
            self._logger.debug('No {:s} sensor definition found'.format(sensor))
            return None

        sensor_def = self._nc_sensor_defs[sensor]
        for required_key in REQUIRED_SENSOR_DEFS_KEYS:
            if required_key not in sensor_def:
                self._logger.warning(
                    '{:s} sensor definition is missing the required key: {:s}'.format(sensor, required_key))
                sensor_def = None

        return sensor_def

    def set_datatype(self, sensor):
        """ Create the sensor NetCDF variable using the specified sensor definition

        Parameters:
            sensor: native glider sensor name
            sensor_def: sensor definition from self._nc_sensor_defs[sensor]

        Returns:
            True if the variable was created or None if the variable is not created
        """

        sensor_def = self.sensor_def_exists(sensor)
        if not sensor_def:
            return  # Skip empty configurations

        if sensor_def['nc_var_name'] in self._nc.variables:
            self._logger.debug('NetCDF variable {:s} already exists'.format(sensor_def['nc_var_name']))
            return  # This variable already exists

        # Confirm dimension
        if 'dimension' not in sensor_def:
            self._logger.warning('{:s} sensor definition has no dimension key'.format(sensor))
            return
        elif not sensor_def['dimension'] or sensor_def['dimension'] is None:
            dimension = ()
        elif type(sensor_def['dimension']) == list:
            dimension = tuple(sensor_def['dimension'])
        else:
            dimension = (sensor_def['dimension'],)

        if 'attrs' not in sensor_def:
            sensor_def['attrs'] = {}

        # Check for user-specified _FillValue or missing_value
        if '_FillValue' in sensor_def['attrs'] and sensor_def['attrs']['_FillValue']:
            var_fill_value = sensor_def['attrs']['_FillValue']
        elif 'missing_value' in sensor_def['attrs'] and sensor_def['attrs']['missing_value']:
            var_fill_value = sensor_def['attrs']['missing_value']
        else:
            try:
                var_fill_value = NC_FILL_VALUES[sensor_def['type']]
            except KeyError as e:
                self._logger.error('Invalid netCDF4 _FillValue type for {:s}: {:s}'.format(sensor, sensor_def['type']))
                return

        try:
            nc_var = self._nc.createVariable(
                sensor_def['nc_var_name'],
                sensor_def['type'],
                dimensions=dimension,
                zlib=True,
                complevel=self._comp_level,
                fill_value=var_fill_value
            )
        except (AttributeError, ValueError) as e:
            self._logger.error('Error in set_datatype for variable {:s}: {:}'.format(sensor, e))
            self._logger.warning('Dimension for {:s} is {:s}'.format(sensor, self._record_dimension['nc_var_name']))
            return

        # Add an attribute to note the variable name used in the source data file
        if 'long_name' not in sensor_def['attrs'] or not sensor_def['attrs']['long_name'].strip():
            sensor_def['attrs']['long_name'] = sensor
        for k, v in sorted(sensor_def['attrs'].items()):
            # self._logger.info('Setting attribute {:s}:{:s}'.format(desc['nc_var_name'], k))
            if k.lower() == '_fillvalue' or k.lower() == 'missing_value':
                continue
            nc_var.setncattr(k, v)

        return sensor_def

    def set_instruments(self):
        """ Adds a list of instrument descriptions to the dataset
        """

        for description in self._attributes['instruments']:
            self._set_instrument(
                description['nc_var_name'],
                description['type'],
                description['attrs']
            )

    def _set_instrument(self, name, var_type, attrs):
        """ Adds a description for a single instrument
        """

        if name not in self._nc.variables:
            self._nc.createVariable(
                name,
                var_type,
                fill_value=NC_FILL_VALUES[var_type]
            )

        for key, value in sorted(attrs.items()):
            self._nc.variables[name].setncattr(key, value)

    def set_trajectory_id(self):
        """ Sets or updates the trajectory dimension and variable for the dataset 
        and the global id attribute

        Input:
            - glider: Name of the glider deployed.
            - deployment_date: String or DateTime of when glider was
                first deployed.
        """

        if 'trajectory' not in self._nc.variables:
            # Setup Trajectory Dimension
            self._nc.createDimension('traj_strlen', len(self._trajectory))

            # Setup Trajectory Variable
            trajectory_var = self._nc.createVariable(
                u'trajectory',
                'S1',
                ('traj_strlen',),
                zlib=True,
                complevel=self._comp_level
            )

            attrs = {
                'cf_role': 'trajectory_id',
                'long_name': 'Trajectory/Deployment Name',  # NOQA
                'comment': 'A trajectory is a single deployment of a glider and may span multiple data files.'  # NOQA
            }
            for key, value in sorted(attrs.items()):
                trajectory_var.setncattr(key, value)
        else:
            trajectory_var = self._nc.variables['trajectory']

        # Set the trajectory variable data
        trajectory_var[:] = stringtoarr(self._trajectory, len(self._trajectory))

        if not self._nc.getncattr('id').strip():
            self._nc.id = self._trajectory  # Global id variable

    def set_source_file_var(self, source_file_string, attrs=None):
        """ Sets the trajectory dimension and variable for the dataset and the
        global id attribute

        Input:
            - glider: Name of the glider deployed.
            - deployment_date: String or DateTime of when glider was
                first deployed.
        """

        if 'source_file' not in self._nc.variables:
            # Setup Trajectory Dimension
            self._nc.createDimension('source_file_strlen', len(source_file_string))

            # Setup Trajectory Variable
            source_file_var = self._nc.createVariable(
                u'source_file',
                'S1',
                ('source_file_strlen',),
                zlib=True,
                complevel=self._comp_level
            )

            if attrs:
                attrs['long_name'] = 'Source data file'
                attrs['comment'] = 'Name of the source data file and associated file metadata'
                for key, value in sorted(attrs.items()):
                    source_file_var.setncattr(key, value)
        else:
            source_file_var = self._nc.variables['source_file']

        # Set the trajectory variable data
        source_file_var[:] = stringtoarr(source_file_string, len(source_file_string))

        if not self._nc.getncattr('source').strip():
            self._nc.source = 'Observational Slocum glider data from source dba file {:s}'.format(
                source_file_string)  # Global source variable

    def insert_var_data(self, var_name, var_data):

        datatype = self.check_datatype_exists(var_name)

        if not datatype:
            self._logger.debug('NetCDF variable {:s} not created'.format(var_name))
            return

        # Add the variable data
        try:
            self._nc.variables[datatype['nc_var_name']][:] = var_data
        except TypeError as e:
            self._logger.error('NetCDF variable {:s}: {:}'.format(var_name, e))
            return

        return True

    def set_array_value(self, key, index, value=None):
        datatype = self.check_datatype_exists(key)

        # Set None or NaN values to _FillValue
        if value is None or np.isnan(value):
            value = NC_FILL_VALUES[datatype['type']]

        self._nc.variables[datatype['nc_var_name']][index] = value

    def delta_to_iso_duration(self, time_delta):
        """Parse a datetime.timedelta object and return a ISO 8601:2004 duration formatted
        string
        """

        seconds = time_delta.total_seconds()
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        days, hours, minutes = map(int, (days, hours, minutes))
        seconds = round(seconds, 6)

        # build date
        date = ''
        if days:
            date = '%sD' % days

        # build time
        time = u'T'
        # hours
        bigger_exists = date or hours
        if bigger_exists:
            time += '{:02}H'.format(hours)
        # minutes
        bigger_exists = bigger_exists or minutes
        if bigger_exists:
            time += '{:02}M'.format(minutes)
        # seconds
        if seconds.is_integer():
            seconds = '{:02}'.format(int(seconds))
        else:
            # 9 chars long w/leading 0, 6 digits after decimal
            seconds = '%09.6f' % seconds
        # remove trailing zeros
        seconds = seconds.rstrip('0')
        time += '{}S'.format(seconds)
        return u'P' + date + time

    def _set_instrument(self, name, var_type, attrs):
        """ Adds a description for a single instrument
        """

        if name not in self._nc.variables:
            self._nc.createVariable(
                name,
                var_type,
                fill_value=NC_FILL_VALUES[var_type]
            )

        for key, value in sorted(attrs.items()):
            self._nc.variables[name].setncattr(key, value)

    def _load_config_sensor_defs(self):
        """Load the configuration path sensor definitions, if self._sensor_def_config_path
        exists
        """

        if not self._sensor_defs_config_path:
            return

        self._logger.debug('Loading deployment sensor definitions: {:s}'.format(self._sensor_defs_config_path))

        try:
            with open(self._sensor_defs_config_path, 'r') as fid:
                self._config_sensor_defs = json.load(fid)
        except ValueError as e:
            self._logger.error('Error parsing deployment-specific sensor definitions: {:s} ({:})'.format(
                self._sensor_defs_config_path, e))
            self._nc_sensor_defs = {}
            self._default_sensor_defs = None

    def update_sensor_def(self, sensor, new_def, override=False):
        """Adds mising key/value pairs in the nc._nc_sensor_defs['sensor']['attrs']
        sensor definition with the key,value pairs in new_def['attrs'].  The only
        exception is the 'units' attribute.  The value for this attribute is left
        as specified in the sensor_defs.json file to allow for proper UDUNITS.
        """

        if sensor not in self._nc_sensor_defs:
            # self._logger.debug('Adding sensor definition: {:s}'.format(sensor))
            self._nc_sensor_defs[sensor] = new_def
            return

        if 'attrs' not in new_def:
            # self._logger.warning('DBA sensor def missing attributes: {:s}'.format(sensor))
            return

        if 'attrs' not in self._nc_sensor_defs[sensor]:
            self._logger.warning('Creating sensor attributes dictionary: {:s}'.format(sensor))
            self._nc_sensor_defs[sensor]['attrs'] = {}

        for k, v in new_def['attrs'].items():
            if k in self._nc_sensor_defs:
                continue

            if k in self._nc_sensor_defs[sensor]['attrs'] and self._nc_sensor_defs[sensor]['attrs'][k]:
                if not override:
                    continue
                else:
                    self._logger.debug('Replacing existing {:s} variable attribute: {:s}'.format(sensor, k))

            self._nc_sensor_defs[sensor]['attrs'][k] = v

    def update_data_file_sensor_defs(self, sensor_defs, override=False):
        """Update the NetCDF sensor definitions with any additional attributes
        created from parsing the raw data file.  If override is set to True, existing attributes from
        self.nc_sensor_defs[sensor] are replaced with the corresponding attribute from the raw data file reader variable
        attributes.
        """

        # Add the derived sensor definitions
        dba_sensors = [s['sensor_name'] for s in sensor_defs]
        for sensor in dba_sensors:
            if sensor not in self.nc_sensor_defs:
                continue

            self.update_sensor_def(sensor, sensor_defs[dba_sensors.index(sensor)], override=override)

    def _update_sensor_defs(self):
        """Updates the sensor definition dicts in self._default_sensor_defs
        with key,value pairs in self._config_sensor_defs if the key is missing or
        if the value mapped to the key is changed
        """

        self._nc_sensor_defs = deepcopy(self._default_sensor_defs)

        if not self._config_sensor_defs:
            self._logger.debug('Instance contains no configured sensor definitions')
            self._configure_record_dimension()
            return

        for sensor, config_sensor_def in self._config_sensor_defs.items():
            self.update_sensor_def(sensor, config_sensor_def)

        # Set record dimension
        self._configure_record_dimension()

    def _configure_record_dimension(self):
        """Check self._nc_sensor_defs for the record dimension
        """

        # Check for the unlimited record dimension after all sensor defs have been
        # updated
        dims = [self._nc_sensor_defs[s] for s in self._nc_sensor_defs if
                'is_dimension' in self._nc_sensor_defs[s] and self._nc_sensor_defs[s]['is_dimension']]
        if not dims:
            self._logger.warning('No record dimension specified in sensor definitions')
            self._logger.warning('Cannot write NetCDF data until a record dimension is defined')
            return

        if len(dims) != 1:
            self._logger.warning('Multiple record dimensions specified in sensor definitions')
            for dim in dims:
                self._logger.warning('Record dimension: {:s}'.format(dim['nc_var_name']))
            self._logger.warning('Only one record dimension is allowed')

        self._record_dimension = dims[0]

    def _read_configs(self):
        """Load in deployment specific configuration files
        """

        self._logger.debug('Loading deployment configuration: {:s}'.format(self._deployment_config_path))
        try:
            with open(self._deployment_config_path, 'r') as fid:
                deployment_configs = json.load(fid)
        except ValueError as e:
            self._logger.error('Error loading {:s}: {:}'.format(self._deployment_config_path, e))
            return

        self._logger.debug('Loading global attributes: {:s}'.format(self._global_attributes_path))
        try:
            with open(self._global_attributes_path, 'r') as fid:
                global_atts = json.load(fid)

        except ValueError as e:
            self._logger.error('Error loading {:s}: {:}'.format(self._global_attributes_path, e))
            return

        self._logger.debug('Loading instrument configurations: {:s}'.format(self._instruments_config_path))
        try:
            with open(self._instruments_config_path, 'r') as fid:
                instrument_configs = json.load(fid)
        except ValueError as e:
            self._logger.error('Error loading {:s}: {:}'.format(self._instruments_config_path, e))
            return

        self._attributes['deployment'] = deployment_configs
        self._attributes['global'] = global_atts
        if 'global_attributes' in deployment_configs and deployment_configs['global_attributes']:
            self._attributes['global'].update(deployment_configs['global_attributes'])
        self._attributes['instruments'] = instrument_configs

        return True

    def __repr__(self):

        return '<NetCDFWriter(config_path={:s}, trajectory=\'{:s}\', format={:s})>'.format(self._config_path,
                                                                                           self._trajectory,
                                                                                           self._nc_format)

    class GliderNetCDFWriterError(Exception):
        pass
