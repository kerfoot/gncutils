#!/usr/bin/env python

import os
import sys
import tempfile
import shutil
from gncutils.readers.dba import create_llat_dba_reader
from gncutils.TrajectoryNetCDFWriter import TrajectoryNetCDFWriter
import logging
import argparse
from gncutils.constants import NETCDF_FORMATS


def main(args):
    """Parse one or more Slocum glider ascii dba files and write CF-compliant Trajectory NetCDF files
    """

    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    config_path = args.config_path
    output_path = args.output_path or os.path.realpath(os.curdir)
    dba_files = args.dba_files
    clobber = args.clobber
    comp_level = args.compression
    nc_format = args.nc_format

    if not os.path.isdir(config_path):
        logging.error('Invalid configuration directory: {:s}'.format(config_path))
        return 1

    if not output_path:
        args.output_path = os.path.realpath(os.curdir)
        logging.info('No NetCDF output_path specified. Using cwd: {:s}'.format(output_path))

    if not os.path.isdir(output_path):
        logging.error('Invalid output_path: {:s}'.format(output_path))
        return 1

    if not dba_files:
        logging.error('No Slocum dba files specified')
        return 1

    # Create the Trajectory NetCDF writer
    ncw = TrajectoryNetCDFWriter(config_path, comp_level=comp_level, nc_format=nc_format, clobber=clobber)
    # -x and the writer configures properly, print to stdout and exit
    if args.debug:
        sys.stdout.write('{}\n'.format(ncw))
        return 0

    # Create a temporary directory for creating/writing NetCDF prior to
    # moving them to output_path
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

        # Split the filename and extension
        dba_filename, dba_ext = os.path.splitext(os.path.basename(dba_file))

        # Parse the dba file
        dba = create_llat_dba_reader(dba_file)
        if len(dba['data']) == 0:
            logging.warning('Skipping empty dba file: {:s}'.format(dba_file))
            continue

        # Create the output NetCDF path
        out_nc_file = os.path.join(output_path,
                                   '{:s}_{:s}.nc'.format(dba['file_metadata']['filename'].replace('-', '_'),
                                                         dba['file_metadata']['filename_extension']))

        # Clobber existing files as long as self._clobber == True.  If not, skip this file
        if os.path.isfile(out_nc_file):
            if args.clobber:
                logging.info('Clobbering existing NetCDF: {:s}'.format(out_nc_file))
            else:
                logging.warning('Skipping existing NetCDF: {:s}'.format(out_nc_file))
                continue

        # Path to hold file while we create it
        tmp_fid, tmp_nc = tempfile.mkstemp(dir=tmp_dir, suffix='.nc', prefix=os.path.basename(__file__))
        os.close(tmp_fid)

        try:
            ncw.init_nc(tmp_nc)
        except (OSError, IOError) as e:
            logging.error('Error initializing {:s}: {}'.format(tmp_nc, e))
            continue

        try:
            ncw.open_nc()
            # Add command line call used to create the file
            ncw.update_history('{:s} {:s}'.format(sys.argv[0], dba_file))
        except (OSError, IOError) as e:
            logging.error('Error opening {:s}: {}'.format(tmp_nc, e))
            os.unlink(tmp_nc)
            continue

        # Create and set the trajectory
        trajectory_string = '{:s}'.format(ncw.trajectory)
        ncw.set_trajectory_id(trajectory_string)
        # Update the global title attribute with the name of the source dba file
        ncw.set_title('Slocum Glider dba file {:s}_{:s}'.format(dba['file_metadata']['filename'].replace('-', '_'),
                                                                   dba['file_metadata']['filename_extension']))

        # Create the source file scalar variable
        ncw.set_source_file_var(dba['file_metadata']['filename_label'], dba['file_metadata'])

        # Update the self.nc_sensors_defs with the dba sensor definitions
        ncw.update_data_file_sensor_defs(dba['sensors'])

        # Find and set container variables
        ncw.set_container_variables()

        # Create variables and add data
        for v in range(len(dba['sensors'])):
            var_name = dba['sensors'][v]['sensor_name']
            var_data = dba['data'][:, v]

            ncw.insert_var_data(var_name, var_data)

        # Permanently close the NetCDF file after writing it
        nc_file = ncw.finish_nc()

        # Add the output NetCDF file name to the list of those to be moved to args.output_dir
        if nc_file:
            try:
                shutil.move(tmp_nc, out_nc_file)
            except IOError as e:
                logging.error('Error moving temp NetCDF file {:s}: {:s}'.format(tmp_nc, e))
                continue

        output_nc_files.append(out_nc_file)
        processed_dbas.append(dba_file)

    # Print the list of files created
    for output_nc_file in output_nc_files:
        os.chmod(output_nc_file, 0o664)
        sys.stdout.write('{:s}\n'.format(output_nc_file))

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('config_path',
                            help='Location of deployment configuration files')

    arg_parser.add_argument('dba_files',
                            help='Source slocum ASCII dba files to process',
                            nargs='+')

    arg_parser.add_argument('-o', '--output_path',
                            help='NetCDF destination directory, which must exist. Default is cwd')

    arg_parser.add_argument('-c', '--clobber',
                            help='Clobber existing NetCDF files if they exist',
                            action='store_true')

    arg_parser.add_argument('-f', '--format',
                            dest='nc_format',
                            help='NetCDF file format',
                            choices=NETCDF_FORMATS,
                            default='NETCDF4_CLASSIC')

    arg_parser.add_argument('--compression',
                            help='NetCDF4 compression level',
                            choices=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                            default=1)

    arg_parser.add_argument('-x', '--debug',
                            help='Check configuration and create NetCDF file writer, but does not process any files',
                            action='store_true')

    arg_parser.add_argument('-l', '--loglevel',
                            help='Verbosity level <Default=info>',
                            type=str,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
