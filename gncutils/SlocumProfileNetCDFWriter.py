import os
import sys
import tempfile
import datetime
import shutil
import logging
import numpy as np
from gncutils.readers.dba import create_llat_dba_reader
from gncutils.yo import slice_sensor_data, find_profiles
from gncutils.validate import validate_sensors, validate_ngdac_var_names
from gncutils.ctd import calculate_practical_salinity, calculate_density, calculate_depth
from gncutils.ProfileNetCDFWriter import ProfileNetCDFWriter


class SlocumProfileNetCDFWriter(ProfileNetCDFWriter):

    '''
    We want to inherit from TrajectoryNetCDFWriter but also add a few more properties,
    specific to writing profile NetCDFs, so we have to call
    TrajectoryNetCDFWriter.__init__(self, ...)
    '''
    def __init__(self, config_path, nc_format='NETCDF4_CLASSIC', comp_level=1,
                 clobber=False, profile_id=0, ctd_sensors=[]):

        ProfileNetCDFWriter.__init__(self, config_path, nc_format=nc_format, comp_level=comp_level, clobber=clobber, profile_id=profile_id)

        self._ctd_sensors = ctd_sensors


    def write_ngdac_profiles(self, dba_files, output_path):
        """
        Loop through the input dba files and write to IOOS NGDAC-compliant Profile NetCDF files

        :param list dba_files - A list of strings containing paths to the input raw glider dba files
        :param str output_path - Path to the output directory
        """
        # Make sure we have llat_* sensors defined in self.nc_sensor_defs
        ctd_valid = validate_sensors(self.nc_sensor_defs, self._ctd_sensors)
        if not ctd_valid:
            logging.error('Bad sensor definitions: {:s}'.format(self.sensor_defs_file))
            return 1
        # Make sure we have configured sensor definitions for all IOOS NGDAC required variables
        ngdac_valid = validate_ngdac_var_names(self.nc_sensor_defs)
        if not ngdac_valid:
            logging.error('Bad sensor definitions: {:s}'.format(self.sensor_defs_file))
            return 1

        # if args.debug:
        #     sys.stdout.write('{}\n'.format(ncw))
        #     return 0

        # Create a temporary directory for creating/writing NetCDF prior to moving them to output_path
        tmp_dir = tempfile.mkdtemp()
        logging.debug('Temporary NetCDF directory: {:s}'.format(tmp_dir))

        # Write one NetCDF file for each input file
        output_nc_files = []
        processed_dbas = []
        for dba_file in dba_files:

            if not os.path.isfile(dba_file):
                logging.error('Invalid dba file specified: {:s}'.format(dba_file))
                continue

            logging.info('Processing dba file: {:s}'.format(dba_file))

            # Parse the dba file
            dba = create_llat_dba_reader(dba_file)
            if dba is None or len(dba['data']) == 0:
                logging.warning('Skipping empty dba file: {:s}'.format(dba_file))
                continue

            # Create the yo for profile indexing find the profile minima/maxima
            yo = slice_sensor_data(dba)
            if yo is None:
                continue
            try:
                profile_times = find_profiles(yo)
            except ValueError as e:
                logging.error('{:s}: {:s}'.format(dba_file, e))
                continue

            if len(profile_times) == 0:
                logging.info('No profiles indexed: {:s}'.format(dba_file))
                continue

            # Pull out llat_time, llat_pressure, llat_lat, llat_lon, sci_water_temp and sci_water_cond to calculate:
            # - depth
            # - salinity
            # - density
            ctd_data = slice_sensor_data(dba, sensors=self._ctd_sensors)
            # Make sure we have latitudes and longitudes
            if np.all(np.isnan(ctd_data[:, 2])):
                logging.warning('dba contains no valid llat_latitude values'.format(dba_file))
                logging.info('Skipping dba: {:s}'.format(dba_file))
                continue
            if np.all(np.isnan(ctd_data[:, 3])):
                logging.warning('dba contains no valid llat_longitude values'.format(dba_file))
                logging.info('Skipping dba: {:s}'.format(dba_file))
                continue
            # Calculate mean llat_latitude and mean llat_longitude
            mean_lat = np.nanmean(ctd_data[:, 2])
            mean_lon = np.nanmean(ctd_data[:, 3])
            # Calculate practical salinity
            prac_sal = calculate_practical_salinity(ctd_data[:, 5], ctd_data[:, 6], ctd_data[:, 1])
            # Add salinity to the dba
            dba['sensors'].append({'attrs': self.nc_sensor_defs['salinity']['attrs'], 'sensor_name': 'salinity'})
            dba['data'] = np.append(dba['data'], np.expand_dims(prac_sal, axis=1), axis=1)

            # Calculate density
            density = calculate_density(ctd_data[:, 0], ctd_data[:, 5], ctd_data[:, 1], prac_sal, mean_lat, mean_lon)
            # Add density to the dba
            dba['sensors'].append({'attrs': self.nc_sensor_defs['density']['attrs'], 'sensor_name': 'density'})
            dba['data'] = np.append(dba['data'], np.expand_dims(density, axis=1), axis=1)

            # Calculate depth from pressure and replace the old llat_depth
            zi = [s['sensor_name'] for s in dba['sensors']].index('llat_depth')
            dba['data'][:, zi] = calculate_depth(ctd_data[:, 1], mean_lat)

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
                    logging.warning('Profile mean timestamp is Nan')
                    continue
                # If no start profile id was specified on the command line, use the mean_profile_epoch as the profile_id
                # since it will be unique to this profile and deployment
                if self.profile_id < 1:
                    self.profile_id = int(mean_profile_epoch)
                pro_mean_dt = datetime.datetime.utcfromtimestamp(mean_profile_epoch)

                # Create the output NetCDF path
                pro_mean_ts = pro_mean_dt.strftime('%Y%m%dT%H%M%SZ')
                profile_filename = '{:s}-{:s}-{:s}-profile'.format(self.attributes['deployment']['glider'], pro_mean_ts,
                                                                   dba['file_metadata']['filename_extension'])
                # Path to temporarily hold file while we create it
                tmp_fid, tmp_nc = tempfile.mkstemp(dir=tmp_dir, suffix='.nc', prefix=os.path.basename(profile_filename))
                os.close(tmp_fid)

                out_nc_file = os.path.join(output_path, '{:s}.nc'.format(profile_filename))
                if os.path.isfile(out_nc_file):
                    if self._clobber:
                        logging.info('Clobbering existing NetCDF: {:s}'.format(out_nc_file))
                    else:
                        logging.warning('Skipping existing NetCDF: {:s}'.format(out_nc_file))
                        continue

                # Initialize the temporary NetCDF file
                try:
                    self.init_nc(tmp_nc)
                except (OSError, IOError) as e:
                    logging.error('Error initializing {:s}: {}'.format(tmp_nc, e))
                    continue

                try:
                    self.open_nc()
                    # Add command line call used to create the file
                    self.update_history('{:s} {:s}'.format(sys.argv[0], dba_file))
                except (OSError, IOError) as e:
                    logging.error('Error opening {:s}: {}'.format(tmp_nc, e))
                    os.unlink(tmp_nc)
                    continue

                # Create and set the trajectory
                # trajectory_string = '{:s}'.format(self.trajectory)
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
                    logging.debug('Inserting {:s} data array'.format(var_name))

                    self.insert_var_data(var_name, var_data)

                # Write scalar profile variable and permanently close the NetCDF file
                nc_file = self.finish_nc()

                if nc_file:
                    try:
                        shutil.move(tmp_nc, out_nc_file)
                        os.chmod(out_nc_file, 0o755)
                    except IOError as e:
                        logging.error('Error moving temp NetCDF file {:s}: {:}'.format(tmp_nc, e))
                        continue

                output_nc_files.append(out_nc_file)

            processed_dbas.append(dba_file)

        # Delete the temporary directory once files have been moved
        try:
            logging.debug('Removing temporary directory: {:s}'.format(tmp_dir))
            shutil.rmtree(tmp_dir)
        except OSError as e:
            logging.error(e)
            return 1

        # Print the list of files created
        for output_nc_file in output_nc_files:
            os.chmod(output_nc_file, 0o664)
            sys.stdout.write('{:s}\n'.format(output_nc_file))

    def __repr__(self):

        return '<ProfileNetCDFWriter(config_path={:s}, trajectory={:s}, format={:s})>'.format(self._config_path,
                                                                                              self._trajectory,
                                                                                              self._format)
