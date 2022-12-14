#!/usr/bin/env python

import os
import sys
import logging
import argparse
import tempfile
import shutil
from gncutils.writers.slocum.ProfileNetCDFWriter import ProfileNetCDFWriter as SlocumProfileNetCDFWriter
from gncutils.readers.slocum import create_llat_dba_reader, derive_ctd_parameters
from gncutils.yo import build_yo, find_profiles
from gncutils.constants import NETCDF_FORMATS


def main(args):
    """Parse one or more Slocum glider ascii dba files and write CF-compliant Profile NetCDF files
    """

    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(asctime)s:%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    config_path = args.config_path
    output_path = args.output_path or os.path.realpath(os.curdir)
    dba_files = args.dba_files
    start_profile_id = args.start_profile_id
    clobber = args.clobber
    comp_level = args.compression
    nc_format = args.nc_format
    ngdac_extensions = args.ngdac
    debug = args.debug

    if debug:
        logging.info('Debug mode: No files will be processed')

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
    ncw = SlocumProfileNetCDFWriter(config_path, comp_level=comp_level, nc_format=nc_format,
                                    profile_id=start_profile_id,
                                    clobber=clobber)

    if not args.clobber:
        logging.info('Keeping existing NetCDF files')

    output_nc_files = []
    if not args.science:
        logging.info('Writing unprocessed NetCDF files')
        if debug:
            sys.stdout.write('{}\n'.format(ncw))
            return 0
        output_nc_files = ncw.dbas_to_profile_nc(dba_files, output_path, ngdac_extensions=ngdac_extensions)
    else:

        logging.info('Writing science NetCDF files')
        if debug:
            sys.stdout.write('{}\n'.format(ncw))
            return 0
        # Create a temporary directory for creating/writing NetCDF prior to moving them to output_path
        tmp_dir = tempfile.mkdtemp()
        logging.debug('Temporary NetCDF directory: {:s}'.format(tmp_dir))

        for dba_file in dba_files:

            logging.info('Processing dba: {:}'.format(dba_file))

            dba = create_llat_dba_reader(dba_file)
            if dba is None:
                continue

            # Create the yo for profile indexing find the profile minima/maxima
            yo = build_yo(dba)
            if yo is None:
                continue
            try:
                profile_times = find_profiles(yo)
            except ValueError as e:
                logging.error('{:s}: {:s}'.format(dba, e))
                continue

            # logging.info('{:0.0f} profiles indexed'.format(len(profile_times)))
            if len(profile_times) == 0:
                continue

            # Derive and add CTD parameters to the dba object
            dba = derive_ctd_parameters(dba)

            # Update the sensor definitions with the calculated parameters
            ncw.update_data_file_sensor_defs(dba['sensors'], override=False)

            nc_files = ncw.dba_obj_to_profile_nc(dba, output_path, tmp_dir=tmp_dir, ngdac_extensions=ngdac_extensions)
            if nc_files:
                output_nc_files = output_nc_files + nc_files

        logging.info('{:0.0f} NetCDF files written: {:s}'.format(len(output_nc_files), output_path))
        # Remove tmp_dir
        try:
            logging.debug('Removing temporary directory: {:s}'.format(tmp_dir))
            shutil.rmtree(tmp_dir)
        except OSError as e:
            logging.error(e)

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
                            help='Source ASCII dba files to process',
                            nargs='+')

    arg_parser.add_argument('-s', '--science',
                            help='Calculate derived scientific parameters and add to NetCDF provided the sensor '
                                 'definitions exist',
                            action='store_true')

    arg_parser.add_argument('--ngdac',
                            help='Name output files using IOOS NGDAC naming conventions. If specified, rt is appended '
                                 'to created NetCDF files for sbd/tbd pairs',
                            action='store_true')

    arg_parser.add_argument('-p', '--start_profile_id',
                            help='Integer specifying the beginning profile id. Default is mean profile unix timestamp',
                            type=int,
                            default=0)

    arg_parser.add_argument('-o', '--output_path',
                            help='NetCDF destination directory, which must exist. Current directory if not specified')

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
                            help='Verbosity level',
                            type=str,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
