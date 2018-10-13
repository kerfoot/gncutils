from gncutils.writers.BaseTrajectoryNetCDFWriter import BaseTrajectoryNetCDFWriter
import tempfile
import os
from gncutils.readers.slocum import create_llat_dba_reader
import numpy as np
import sys
import shutil
from gncutils.constants import NC_FILL_VALUES


class TrajectoryNetCDFWriter(BaseTrajectoryNetCDFWriter):

    def __init__(self, config_path, nc_format='NETCDF4_CLASSIC', comp_level=1, clobber=False):
        BaseTrajectoryNetCDFWriter.__init__(self, config_path, nc_format=nc_format, comp_level=comp_level,
                                            clobber=clobber)

    def dbas_to_trajectory_nc(self, dba_files, output_path, z_from_p=True, ngdac_extensions=False):

        # Create a temporary directory for creating/writing NetCDF prior to
        # moving them to output_path
        tmp_dir = tempfile.mkdtemp()
        self._logger.debug('Temporary NetCDF directory: {:s}'.format(tmp_dir))

        # Write one NetCDF file for each input file
        output_nc_files = []
        processed_dbas = []
        for dba_file in dba_files:

            if not os.path.isfile(dba_file):
                self._logger.error('Invalid dba file specified: {:s}'.format(dba_file))
                continue

            self._logger.debug('Reading dba file: {:s}'.format(dba_file))

            # Parse the dba file
            dba = create_llat_dba_reader(dba_file, z_from_p=z_from_p)
            if not dba or len(dba['data']) == 0:
                self._logger.warning('Skipping empty dba file: {:s}'.format(dba_file))
                continue

            # Create the output NetCDF path
            if ngdac_extensions:
                telemetry = 'rt'
                if dba['file_metadata']['filename_extension'] != 'sbd' and dba['file_metadata']['filename_extension']:
                    telemetry = 'delayed'

                trajectory_nc_file = '{:s}_{:s}'.format(dba['file_metadata']['filename'].replace('-', '_'), telemetry)
            else:
                trajectory_nc_file = '{:s}_{:s}'.format(dba['file_metadata']['filename'].replace('-', '_'),
                                                 dba['file_metadata']['filename_extension'])

            # Fully qualified path to the output file
            out_nc_file = os.path.join(output_path, '{:s}.nc'.format(trajectory_nc_file))

            # Clobber existing files as long as self._clobber == True.  If not, skip this file
            if os.path.isfile(out_nc_file):
                if self._clobber:
                    self._logger.warning('Clobbering existing NetCDF: {:s}'.format(out_nc_file))
                else:
                    self._logger.debug('Skipping existing NetCDF: {:s}'.format(out_nc_file))
                    continue

            self._logger.info('Processing dba file: {:s}'.format(dba_file))

            # Path to hold file while we create it
            tmp_fid, tmp_nc = tempfile.mkstemp(dir=tmp_dir, suffix='.nc', prefix=os.path.basename(__file__))
            os.close(tmp_fid)

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

            self.set_trajectory_id()
            # Update the global title attribute with the name of the source dba file
            self.set_title('Slocum Glider dba file {:s}_{:s}'.format(dba['file_metadata']['filename'].replace('-', '_'),
                                                                     dba['file_metadata']['filename_extension']))

            # Create the source file scalar variable
            self.set_source_file_var(dba['file_metadata']['filename_label'], dba['file_metadata'])

            # Update the self.nc_sensors_defs with the dba sensor definitions
            self.update_data_file_sensor_defs(dba['sensors'])

            # Find and set container variables
            self.set_container_variables()

            # Clean up the dba:
            # 1. Replace NaNs with fill values
            # 2. Set llat_time 0 values to fill values
            dba = self.clean_dba(dba)

            # Create variables and add data
            for v in range(len(dba['sensors'])):
                var_name = dba['sensors'][v]['sensor_name']

                sensor_def = self.sensor_def_exists(var_name)
                if not sensor_def:
                    continue

                # Pull out the data
                var_data = dba['data'][:, v]

                # # Default fill value for slocum gliders is nan.  Check the sensor def for 'attrs'['_FillValue'].  If it
                # # doesn't exist, get the default fill value for this datatype replace all NaNs with this value and
                # # update the sensor def
                # fill_value = None
                # if '_FillValue' in sensor_def['attrs']:
                #     fill_value = sensor_def['attrs']['_FillValue']
                # elif 'missing_value' in sensor_def['attrs']:
                #     fill_value = sensor_def['attrs']['missing_value']
                # if '_FillValue' not in sensor_def['attrs'] and 'missing_value' not in sensor_def['attrs']:
                #     try:
                #         fill_value = NC_FILL_VALUES[sensor_def['type']]
                #         sensor_def['attrs']['_FillValue'] = fill_value
                #         self.update_sensor_def(var_name, sensor_def)
                #     except KeyError:
                #         self._logger.error(
                #             'Invalid netCDF4 _FillValue type for {:s}: {:s}'.format(var_name, sensor_def['type']))
                #         return
                #
                # if not fill_value:
                #     continue
                #
                # # Replace NaNs with sensor_def['attrs']['_FillValue']
                # var_data[np.isnan(var_data)] = fill_value

                self.insert_var_data(var_name, var_data)

            # Permanently close the NetCDF file after writing it
            nc_file = self.finish_nc()

            # Add the output NetCDF file name to the list of those to be moved to args.output_dir
            if nc_file:
                try:
                    shutil.move(tmp_nc, out_nc_file)
                except IOError as e:
                    self._logger.error('Error moving temp NetCDF file {:s}: {:s}'.format(tmp_nc, e))
                    continue

            output_nc_files.append(out_nc_file)
            processed_dbas.append(dba_file)

        # Delete the temporary directory once files have been moved
        try:
            self._logger.debug('Removing temporary directory: {:s}'.format(tmp_dir))
            shutil.rmtree(tmp_dir)
        except OSError as e:
            self._logger.error('{:}'.format(e))

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
            fill_value = None
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

