from gncutils.writers.BaseProfileNetCDFWriter import BaseProfileNetCDFWriter
import tempfile
import os
from gncutils.readers.navoceano import create_llat_nc_reader
from gncutils.yo import slice_sensor_data, find_profiles
import numpy as np
import datetime
import sys
from netCDF4 import Dataset
import shutil


class ProfileNetCDFWriter(BaseProfileNetCDFWriter):

    def __init__(self, config_path, nc_format='NETCDF4_CLASSIC', comp_level=1, clobber=False, profile_id=1):
        BaseProfileNetCDFWriter.__init__(self, config_path, nc_format=nc_format, comp_level=comp_level, clobber=clobber)

    def navonc_to_ngdacnc(self, navo_nc_files, output_path):

        # Create a temporary directory for creating/writing NetCDF prior to moving them to output_path
        tmp_dir = tempfile.mkdtemp()
        self._logger.debug('Temporary NetCDF directory: {:s}'.format(tmp_dir))

        # Write one NetCDF file for each input file
        output_nc_files = []
        processed_ncs = []
        for nc_file in navo_nc_files:

            if not os.path.isfile(nc_file):
                self._logger.error('Invalid NAVO NetCDF file specified: {:s}'.format(nc_file))
                continue

            self._logger.info('Processing NAVO NetCDF file: {:s}'.format(nc_file))

            # Parse the dba file
            dba = create_llat_nc_reader(nc_file)
            if dba is None or len(dba['data']) == 0:
                self._logger.warning('Skipping empty NAVO NetCDF file: {:s}'.format(nc_file))
                continue

            # NAVOCEANO NetCDF files contain missing_values in the time coordinate variable (dimension). We need to remove
            # them
            ti = [s['sensor_name'] for s in dba['sensors']].index('llat_time')
            missing_value = dba['sensors'][ti]['attrs']['missing_value']
            dba['data'] = dba['data'][dba['data'][:, ti] != missing_value, :]

            # Create the yo for profile indexing find the profile minima/maxima
            yo = slice_sensor_data(dba)
            if yo is None:
                continue
            try:
                profile_times = find_profiles(yo)
            except ValueError as e:
                self._logger.error('{:s}: {:s}'.format(nc_file, e))
                continue

            if len(profile_times) == 0:
                self._logger.info('No profiles indexed: {:s}'.format(nc_file))
                continue

            # All timestamps from stream
            ts = yo[:, 0]

            # Open up the source NetCDF file
            nci = Dataset(nc_file, 'r')
            # Add the source NetCDF file's global attributes to the
            source_global_atts = nci.ncattrs()
            for att in source_global_atts:
                self.add_global_attribute(att, nci.getncattr(att), override=False)

            # Update the self.nc_sensors_defs with the dba sensor definitions
            self.update_data_file_sensor_defs(dba['sensors'])

            # The NAVOCEANO NetCDF files have a global attribute specifying the dive number.  Use this number as the
            # first profile id and then increment it for successive dives
            # start_profile_id = dba['file_metadata']['dive_number']
            profile_count = 0
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
                # Set the profile id
                # self.profile_id = start_profile_id
                # Increment the profile counter
                # start_profile_id += 1
                # Use the mean_profile_epoch as the profile_id
                self.profile_id = mean_profile_epoch
                pro_mean_dt = datetime.datetime.utcfromtimestamp(mean_profile_epoch)

                # Create the output NetCDF path
                pro_mean_ts = pro_mean_dt.strftime('%Y%m%dT%H%M%S')
                profile_filename = '{:s}_{:s}_rt'.format(self.attributes['deployment']['glider'], pro_mean_ts)
                # Path to temporarily hold file while we create it
                tmp_fid, tmp_nc = tempfile.mkstemp(dir=tmp_dir, suffix='.nc', prefix=os.path.basename(profile_filename))
                os.close(tmp_fid)

                out_nc_file = os.path.join(output_path, '{:s}.nc'.format(profile_filename))
                if os.path.isfile(out_nc_file):
                    if self.clobber:
                        self._logger.info('Clobbering existing NetCDF: {:s}'.format(out_nc_file))
                    else:
                        self._logger.warning('Skipping existing NetCDF: {:s}'.format(out_nc_file))
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
                    self.update_history('{:s} {:s}'.format(sys.argv[0], nc_file))
                except (OSError, IOError) as e:
                    self._logger.error('Error opening {:s}: {}'.format(tmp_nc, e))
                    os.unlink(tmp_nc)
                    continue

                # Create and set the trajectory
                # trajectory_string = '{:s}'.format(self.trajectory)
                self.set_trajectory_id()
                # Update the global title attribute with the name of the source dba file
                self.set_title('{:s}-{:s} Vertical Profile'.format(self.deployment_configs['glider'],
                                                                  pro_mean_dt.strftime('%Y%m%d%H%M%SZ')))

                # Create the source file scalar variable
                self.set_source_file_var(dba['file_metadata']['source_file'], dba['file_metadata'])

                # Update the self.nc_sensors_defs with the dba sensor definitions
                self.update_data_file_sensor_defs(dba['sensors'])

                # Find and set container variables
                self.set_container_variables()

                # Create variables and add data
                for v in list(range(len(dba['sensors']))):
                    var_name = dba['sensors'][v]['sensor_name']
                    # Make sure there is a sensor definition before attempting to add the data array
                    if var_name not in self.nc_sensor_defs:
                        continue
                    var_data = dba['data'][p_inds, v]
                    self._logger.debug('Inserting {:s} data array'.format(var_name))

                    self.insert_var_data(var_name, var_data)

                # Check for u_da and v_da sensors (u/v currents).  If they exist, add them as variables
                u_sensor_def = self.sensor_def_exists('u_da')
                v_sensor_def = self.sensor_def_exists('v_da')
                uv_time_sensor_def = self.sensor_def_exists('time_uv')
                if u_sensor_def and v_sensor_def and uv_time_sensor_def:
                    # u_var_name = u_sensor_def['nc_var_name']
                    # v_var_name = v_sensor_def['nc_var_name']
                    try:
                        u = self.set_scalar('u_da', nci.variables['u_da'][0])
                    except KeyError as e:
                        self._logger.error('Failed to create u current variable: {:}'.format(e))
                    try:
                        v = self.set_scalar('v_da', nci.variables['v_da'][0])
                    except KeyError as e:
                        self._logger.error('Failed to create v current variable: {:}'.format(e))
                    try:
                        t = self.set_scalar('time_uv', mean_profile_epoch)
                    except KeyError as e:
                        self._logger.error('Failed to create time_uv variable: {:}'.format(e))

                # Write scalar profile variable and permanently close the NetCDF file
                nc_written = self.finish_nc()

                if nc_written:
                    try:
                        shutil.move(tmp_nc, out_nc_file)
                        os.chmod(out_nc_file, 0o775)
                    except IOError as e:
                        self._logger.error('Error moving temp NetCDF file {:s}: {:}'.format(tmp_nc, e))
                        continue

                output_nc_files.append(out_nc_file)

                profile_count += 1

            nci.close()
            processed_ncs.append(nc_file)
            self._logger.info('{:0.0f} profiles written'.format(profile_count))


        # Delete the temporary directory once files have been moved
        try:
            self._logger.debug('Removing temporary directory: {:s}'.format(tmp_dir))
            shutil.rmtree(tmp_dir)
        except OSError as e:
            self._logger.error(e)
            return 1

        return output_nc_files
