from gncutils.writers.BaseProfileNetCDFWriter import BaseProfileNetCDFWriter
import tempfile
import os
from gncutils.readers.slocum import create_llat_dba_reader
from gncutils.yo import slice_sensor_data, build_yo, find_profiles
import numpy as np
import datetime
import sys
import shutil
from gncutils.constants import NC_FILL_VALUES


class ProfileNetCDFWriter(BaseProfileNetCDFWriter):

    def __init__(self, config_path, nc_format='NETCDF4_CLASSIC', comp_level=1, clobber=False, profile_id=1):
        BaseProfileNetCDFWriter.__init__(self, config_path, nc_format=nc_format, comp_level=comp_level, clobber=clobber,
                                         profile_id=profile_id)

    def dbas_to_profile_nc(self, dba_files, output_path, z_from_p=True, ngdac_extensions=False):

        # Create a temporary directory for creating/writing NetCDF prior to moving them to output_path
        tmp_dir = tempfile.mkdtemp()
        self._logger.debug('Temporary NetCDF directory: {:s}'.format(tmp_dir))

        # Write one NetCDF file for each input file
        output_nc_files = []
        processed_dbas = []
        non_clobbered_nc_files_count = 0
        for dba_file in dba_files:

            if not os.path.isfile(dba_file):
                self._logger.error('Invalid dba file specified: {:s}'.format(dba_file))
                continue

            self._logger.info('Processing dba file: {:s}'.format(dba_file))

            # Parse the dba file
            dba = create_llat_dba_reader(dba_file, z_from_p=z_from_p)
            if dba is None or len(dba['data']) == 0:
                self._logger.warning('Skipping empty dba file: {:s}'.format(dba_file))
                continue

            # Create the yo for profile indexing find the profile minima/maxima
            yo = build_yo(dba)
            if yo is None:
                continue
            try:
                profile_times = find_profiles(yo)
            except ValueError as e:
                self._logger.error('{:s}: {:s}'.format(dba_file, e))
                continue

            self._logger.info('{:0.0f} profiles indexed'.format(len(profile_times)))
            if len(profile_times) == 0:
                continue

            # Clean up the dba:
            # 1. Replace NaNs with fill values
            # 2. Set llat_time 0 values to fill values
            dba = self.clean_dba(dba)

            # All timestamps from stream
            ts = yo[:, 0]

            for profile_interval in profile_times:

                # Profile start time
                p0 = profile_interval[0]
                # Profile end time
                p1 = profile_interval[-1]
                # Find all rows in ts that are between p0 & p1
                p_inds = np.flatnonzero(np.logical_and(ts >= p0, ts <= p1))
                # profile_stream = dba['data'][p_inds[0]:p_inds[-1]]

                # Calculate and convert profile mean time to a datetime
                mean_profile_epoch = np.nanmean(profile_interval)
                if np.isnan(mean_profile_epoch):
                    self._logger.warning('Profile mean timestamp is Nan')
                    continue
                # If no start profile id was specified on the command line, use the mean_profile_epoch as the profile_id
                # since it will be unique to this profile and deployment
                if self.profile_id < 1:
                    self.profile_id = int(mean_profile_epoch)
                pro_mean_dt = datetime.datetime.utcfromtimestamp(mean_profile_epoch)

                # Create the output NetCDF path
                pro_mean_ts = pro_mean_dt.strftime('%Y%m%dT%H%M%SZ')
                if ngdac_extensions:
                    telemetry = 'rt'
                    if dba['file_metadata']['filename_extension'] != 'sbd' and dba['file_metadata']['filename_extension']:
                        telemetry = 'delayed'

                    profile_nc_file = '{:s}_{:s}_{:s}'.format(self.attributes['deployment']['glider'],
                                                               pro_mean_ts,
                                                               telemetry)
                else:
                    profile_nc_file = '{:s}_{:s}_{:s}'.format(self.attributes['deployment']['glider'],
                                                               pro_mean_ts,
                                                               dba['file_metadata']['filename_extension'])
                # Path to temporarily hold file while we create it
                tmp_fid, tmp_nc = tempfile.mkstemp(dir=tmp_dir, suffix='.nc', prefix=os.path.basename(profile_nc_file))
                os.close(tmp_fid)

                out_nc_file = os.path.join(output_path, '{:s}.nc'.format(profile_nc_file))
                if os.path.isfile(out_nc_file):
                    if self.clobber:
                        self._logger.info('Clobbering existing NetCDF: {:s}'.format(out_nc_file))
                    else:
                        self._logger.debug('Skipping existing NetCDF: {:s}'.format(out_nc_file))
                        non_clobbered_nc_files_count += 1
                        continue

                # Initialize the temporary NetCDF file
                try:
                    self.init_nc(tmp_nc)
                except (OSError, IOError) as e:
                    self._logger.error('Error initializing {:s}: {}'.format(tmp_nc, e))
                    continue

                try:
                    self.open_nc()
                    # Add command line call used to create the file
                    self.update_history('{:s} {:s}'.format(sys.argv[0], dba_file))
                except (OSError, IOError) as e:
                    self._logger.error('Error opening {:s}: {}'.format(tmp_nc, e))
                    os.unlink(tmp_nc)
                    continue

                # Create and set the trajectory
                trajectory_string = '{:s}'.format(self.trajectory)
                self.set_trajectory_id()
                # Update the global title attribute with the name of the source dba file
                self.set_title('{:s}-{:s} Vertical Profile'.format(self.deployment_configs['glider'],
                                                                   pro_mean_dt.strftime('%Y%m%d%H%M%SZ')))

                # Create the source file scalar variable
                self.set_source_file_var(dba['file_metadata']['filename_label'], dba['file_metadata'])

                # Update the self.nc_sensors_defs with the dba sensor definitions
                self.update_data_file_sensor_defs(dba['sensors'])

                # Find and set container variables
                self.set_container_variables()

                # Create variables and add data
                for v in list(range(len(dba['sensors']))):
                    var_name = dba['sensors'][v]['sensor_name']
                    var_data = dba['data'][p_inds, v]
                    self._logger.debug('Inserting {:s} data array'.format(var_name))

                    self.insert_var_data(var_name, var_data)

                # Write scalar profile variable and permanently close the NetCDF file
                nc_file = self.finish_nc()

                if nc_file:
                    try:
                        shutil.move(tmp_nc, out_nc_file)
                        os.chmod(out_nc_file, 0o755)
                    except IOError as e:
                        self._logger.error('Error moving temp NetCDF file {:s}: {:}'.format(tmp_nc, e))
                        continue

                output_nc_files.append(out_nc_file)

            processed_dbas.append(dba_file)

        if not self.clobber:
            self._logger.info('{:0.0f} NetCDFs not clobbered'.format(non_clobbered_nc_files_count))

        # Delete the temporary directory once files have been moved
        try:
            self._logger.debug('Removing temporary directory: {:s}'.format(tmp_dir))
            shutil.rmtree(tmp_dir)
        except OSError as e:
            self._logger.error(e)
            return 1

        return output_nc_files

    def dba_obj_to_profile_nc(self, dba, output_path, tmp_dir=None, ngdac_extensions=False):

        output_nc_files = []
        non_clobbered_nc_files_count = 0

        # Create the yo for profile indexing find the profile minima/maxima
        yo = build_yo(dba)
        if yo is None:
            return output_nc_files
        try:
            profile_times = find_profiles(yo)
        except ValueError as e:
            self._logger.error('{:s}: {:s}'.format(dba, e))
            return output_nc_files

        self._logger.info('{:0.0f} profiles indexed'.format(len(profile_times)))
        if len(profile_times) == 0:
            return output_nc_files

        if not tmp_dir:
            self._logger.info('Creating temporary NetCDF location')
            try:
                tmp_dir = tempfile.mkdtemp()
            except OSError as e:
                self._logger.error('Error creating temporary NetCDF location: {:}'.format(e))
                return output_nc_files

        self._logger.debug('Temporary NetCDF directory: {:s}'.format(tmp_dir))
        if not os.path.isdir(tmp_dir):
            self._logger.warning('Temporary NetCDF directory does not exist: {:s}'.format(tmp_dir))
            return output_nc_files

        # Clean up the dba:
        # 1. Replace NaNs with fill values
        # 2. Set llat_time 0 values to fill values
        dba = self.clean_dba(dba)

        # All timestamps from stream
        ts = yo[:, 0]

        # Update the self.nc_sensors_defs with the dba sensor definitions
        self.update_data_file_sensor_defs(dba['sensors'])

        for profile_interval in profile_times:

            # Profile start time
            p0 = profile_interval[0]
            # Profile end time
            p1 = profile_interval[-1]
            # Find all rows in ts that are between p0 & p1
            p_inds = np.flatnonzero(np.logical_and(ts >= p0, ts <= p1))
            # profile_stream = dba['data'][p_inds[0]:p_inds[-1]]

            # Calculate and convert profile mean time to a datetime
            mean_profile_epoch = np.nanmean(profile_interval)
            if np.isnan(mean_profile_epoch):
                self._logger.warning('Profile mean timestamp is Nan')
                continue
            # If no start profile id was specified on the command line, use the mean_profile_epoch as the profile_id
            # since it will be unique to this profile and deployment
            if self.profile_id < 1:
                self.profile_id = int(mean_profile_epoch)
            pro_mean_dt = datetime.datetime.utcfromtimestamp(mean_profile_epoch)

            # Create the output NetCDF path
            pro_mean_ts = pro_mean_dt.strftime('%Y%m%dT%H%M%SZ')
            if ngdac_extensions:
                telemetry = 'rt'
                if dba['file_metadata']['filename_extension'] != 'sbd' and dba['file_metadata']['filename_extension']:
                    telemetry = 'delayed'

                profile_nc_file = '{:s}_{:s}_{:s}'.format(self.attributes['deployment']['glider'],
                                                          pro_mean_ts,
                                                          telemetry)
            else:
                profile_nc_file = '{:s}_{:s}_{:s}'.format(self.attributes['deployment']['glider'],
                                                          pro_mean_ts,
                                                          dba['file_metadata']['filename_extension'])
            # Path to temporarily hold file while we create it
            tmp_fid, tmp_nc = tempfile.mkstemp(dir=tmp_dir, suffix='.nc', prefix=os.path.basename(profile_nc_file))
            os.close(tmp_fid)

            out_nc_file = os.path.join(output_path, '{:s}.nc'.format(profile_nc_file))
            if os.path.isfile(out_nc_file):
                if self.clobber:
                    self._logger.info('Clobbering existing NetCDF: {:s}'.format(out_nc_file))
                else:
                    self._logger.debug('Skipping existing NetCDF: {:s}'.format(out_nc_file))
                    non_clobbered_nc_files_count += 1
                    continue

            # Initialize the temporary NetCDF file
            try:
                self.init_nc(tmp_nc)
            except (OSError, IOError) as e:
                self._logger.error('Error initializing {:s}: {}'.format(tmp_nc, e))
                continue

            try:
                self.open_nc()
                # Add command line call used to create the file
                self.update_history('{:s} {:s}'.format(sys.argv[0], dba['file_metadata']['source_file']))
            except (OSError, IOError) as e:
                self._logger.error('Error opening {:s}: {}'.format(tmp_nc, e))
                os.unlink(tmp_nc)
                continue

            # Create and set the trajectory
            trajectory_string = '{:s}'.format(self.trajectory)
            self.set_trajectory_id()
            # Update the global title attribute with the name of the source dba file
            self.set_title('{:s}-{:s} Vertical Profile'.format(self.deployment_configs['glider'],
                                                               pro_mean_dt.strftime('%Y%m%d%H%M%SZ')))

            # Create the source file scalar variable
            self.set_source_file_var(dba['file_metadata']['filename_label'], dba['file_metadata'])

            # # Update the self.nc_sensors_defs with the dba sensor definitions
            # self.update_data_file_sensor_defs(dba['sensors'])

            # Find and set container variables
            self.set_container_variables()

            # Create variables and add data
            for v in list(range(len(dba['sensors']))):
                var_name = dba['sensors'][v]['sensor_name']
                var_data = dba['data'][p_inds, v]
                self._logger.debug('Inserting {:s} data array'.format(var_name))

                self.insert_var_data(var_name, var_data)

            # Write scalar profile variable and permanently close the NetCDF file
            nc_file = self.finish_nc()

            if nc_file:
                try:
                    shutil.move(tmp_nc, out_nc_file)
                    os.chmod(out_nc_file, 0o755)
                except IOError as e:
                    self._logger.error('Error moving temp NetCDF file {:s}: {:}'.format(tmp_nc, e))
                    continue

            output_nc_files.append(out_nc_file)

        if not self.clobber:
            self._logger.info('{:0.0f} NetCDFs not clobbered'.format(non_clobbered_nc_files_count))

        return output_nc_files

    def clean_dba(self, dba):
        """Clean the parsed dba file dict:
            1. Sets _FillValue to netCDF4 python default type _FillValue
            2. Replace NaN values with the _FillValue from #1
            3. Set llat_time 0 values to _FillValue from #1
            """

        # Set _FillValues
        for v in range(len(dba['sensors'])):
            var_name = dba['sensors'][v]['sensor_name']

            sensor_def = self.sensor_def_exists(var_name)
            if not sensor_def:
                continue

            # Pull out the data
            var_data = dba['data'][:, v]

            # Default fill value for slocum gliders is nan.  Check the sensor def for 'attrs'['_FillValue'].  If it
            # doesn't exist, get the default fill value for this datatype replace all NaNs with this value and
            # update the sensor def
            if '_FillValue' in sensor_def['attrs'] and sensor_def['attrs']['_FillValue'] is not None:
                fill_value = sensor_def['attrs']['_FillValue']
            elif 'missing_value' in sensor_def['attrs'] and sensor_def['attrs']['missing_value'] is not None:
                fill_value = sensor_def['attrs']['missing_value']
            # if '_FillValue' not in sensor_def['attrs'] and 'missing_value' not in sensor_def['attrs']:
            else:
                try:
                    fill_value = NC_FILL_VALUES[sensor_def['type']]
                    sensor_def['attrs']['_FillValue'] = fill_value
                    self.update_sensor_def(var_name, sensor_def)
                except KeyError:
                    self._logger.error(
                        'Invalid netCDF4 _FillValue type for {:s}: {:s}'.format(var_name, sensor_def['type']))
                    return

            if not fill_value:
                continue

            # Replace NaNs with sensor_def['attrs']['_FillValue']
            var_data[np.isnan(var_data)] = fill_value

        # Replace llat_time 0 values with _FillValue
        if 'llat_time' not in self._nc_sensor_defs:
            self._logger.warning('No sensor defintion found for llat_time')
            return dba

        fill_value = self._nc_sensor_defs['llat_time']['attrs']['_FillValue']
        sensors = [s['sensor_name'] for s in dba['sensors']]
        if 'llat_time' not in sensors:
            self._logger.warning('llat_time not found in dba: {:s}'.format(dba['file_metadata']['source_file']))
            return dba

        ti = sensors.index('llat_time')
        np.place(dba['data'][:, ti], dba['data'][:, ti] == 0, fill_value)

        return dba
