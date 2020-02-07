#!/usr/bin/env python

import logging
import os
import argparse
import sys
from gncutils.readers.slocum import create_llat_dba_reader
from gncutils.yo import slice_sensor_data, find_yo_extrema
from gncutils.yo.plot import plot_yo
from gncutils.yo.filters import default_profiles_filter


def main(args):
    """Parse the specified Slocum glider dba file and plot the yo and the indexed profiles"""

    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    if not os.path.isfile(args.dba_file):
        logging.error('Invalid dba file specified: {:s}'.format(args.dba_file))
        return 1

    logging.debug('Parsing dba file: {:s}'.format(args.dba_file))
    dba = create_llat_dba_reader(args.dba_file)
    if len(dba['data']) == 0:
        logging.warning('Empty dba file: {:s}'.format(args.dba_file))
        return 1

    # Create the yo for profile indexing find the profile minima/maxima
    logging.debug('Creating depth time-series...')
    yo = slice_sensor_data(dba)
    if yo is None:
        logging.warning('Failed to create depth time-series: {:s}'.format(args.dba_file))
        return 1

    # Index the profiles
    logging.debug('Indexing profiles...')
    profile_times = find_yo_extrema(yo[:, 0], yo[:, 1])
    if len(profile_times) == 0:
        logging.info('No profiles indexed: {:s}'.format(args.dba_file))
        return 0

    if args.clean:
        logging.debug('Cleaning up indexed profiles...')
        profile_times = default_profiles_filter(yo, profile_times)

    logging.debug('Plotting yo...')
    plot_yo(yo, profile_times)

    return 0


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('dba_file',
                            help='Path to the dba file')

    arg_parser.add_argument('-c', '--clean',
                            help='Clean up/filter the indexed profiles',
                            action='store_true')

    arg_parser.add_argument('-l', '--loglevel',
                            help='Verbosity level',
                            type=str,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            default='info')

    parsed_args = arg_parser.parse_args()

    # print(parsed_args)
    # sys.exit(1)

    sys.exit(main(parsed_args))
