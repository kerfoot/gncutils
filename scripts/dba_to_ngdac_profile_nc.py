#!/usr/bin/env python

import os
import sys
import logging
import argparse
from gncutils.SlocumProfileNetCDFWriter import SlocumProfileNetCDFWriter
from gncutils.constants import NETCDF_FORMATS, LLAT_SENSORS


def main(args):
    """Parse one or more Slocum glider ascii dba files and write IOOS NGDAC-compliant Profile NetCDF files
    """

    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    config_path = args.config_path
    output_path = args.output_path or os.path.realpath(os.curdir)
    dba_files = args.dba_files
    start_profile_id = args.start_profile_id
    clobber = args.clobber
    comp_level = args.compression
    nc_format = args.nc_format
    ctd_sensor_prefix = args.ctd_sensor_prefix

    ctd_sensor_names = ['{:s}_{:s}'.format(ctd_sensor_prefix, ctd_sensor) for ctd_sensor in
                        ['water_cond', 'water_temp']]
    ctd_sensors = LLAT_SENSORS + ctd_sensor_names

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
    ncw = SlocumProfileNetCDFWriter(config_path, comp_level=comp_level, nc_format=nc_format, profile_id=start_profile_id,
                                    clobber=clobber, ctd_sensors=ctd_sensors)
    ncw.write_ngdac_profiles(dba_files, output_path)


    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('config_path',
                            help='Location of deployment configuration files')

    arg_parser.add_argument('dba_files',
                            help='Source ASCII dba files to process',
                            nargs='+')

    arg_parser.add_argument('--ctd_sensor_prefix',
                            help='Specify Slocum glider ctd sensor prefix letter, i.e.: sci_water_temp, sci_water_cond',
                            choices=['sci', 'm'],
                            default='sci')

    arg_parser.add_argument('-p', '--start_profile_id',
                            help='Integer specifying the beginning profile id. If not specified or <1 the mean profile unix timestamp is used',
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
