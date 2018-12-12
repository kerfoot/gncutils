#!/usr/bin/env python

import argparse
import os
import logging
import json
import sys
import re


def main(args):
    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    status = 0

    cac_file = args.cac_file
    sensor_defs_file = args.sensor_defs_json
    dimension = args.dimension
    clobber = args.clobber

    if not os.path.isfile(cac_file):
        logging.error('Inavlid .cac file specified: {:s}'.format(cac_file))
        return 1

    if sensor_defs_file:
        if not os.path.isfile(sensor_defs_file):
            logging.error('Invalid sensor definitions file: {:s}'.format(sensor_defs_file))
            return 1

    # Read args.cac_file
    try:
        with open(cac_file, 'r') as fid:
            cac_contents = fid.readlines()
    except IOError as e:
        logging.error('{}'.format(e))
        return 1

    # Read args.sensor_defs_json
    sensor_defs = {}
    if sensor_defs_file:
        try:
            with open(sensor_defs_file, 'r') as fid:
                sensor_defs = json.load(fid)
        except (IOError, ValueError) as e:
            logging.error('{}'.format(e))
            return 1

    if clobber:
        logging.info('Clobbering existing sensor definitions')

    def_regex = re.compile(r'^s:\s+[TF]\s+\d+\s+\-?\d+\s+(\d+)\s+(\w+)\s+(.*)$')
    sensor_count = 0
    for cac_sensor in cac_contents:
        match = def_regex.search(cac_sensor)

        if not match:
            logging.warning('Invalid cac sensor line: {:s}'.format(cac_sensor))
            continue

        sensor_name = match.groups()[1]

        if sensor_name in sensor_defs:
            logging.debug('Sensor definition exists: {:s}'.format(sensor_name))
            if not clobber:
                continue

        dtype = 'f8'
        num_bytes = int(match.groups()[0])
        if num_bytes == 1:
            dtype = 'i1'
        if num_bytes == 2:
            dtype = 'i2'
        elif num_bytes == 4:
            dtype = 'f4'

        units = match.groups()[2]
        sensor_def = {'attrs': {'bytes': num_bytes, 'sensor': sensor_name, 'type': dtype, 'units': units},
                      'dimension': dimension,
                      'nc_var_name': sensor_name,
                      'type': dtype}

        logging.info('Adding sensor definition: {:s}'.format(sensor_name))
        sensor_count += 1

        sensor_defs[sensor_name] = sensor_def

    sys.stdout.write('{:s}\n'.format(json.dumps(sensor_defs, indent=4, sort_keys=True)))
    logging.info('{:0.0f} new sensor definitions added'.format(sensor_count))

    return status


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('cac_file',
                            help='Slocum .cac file to parse')

    arg_parser.add_argument('sensor_defs_json',
                            help='Sensor definitions JSON file',
                            nargs='?')

    arg_parser.add_argument('-d', '--dimension',
                            help='Dimension name',
                            choices=['time', 'obs'],
                            default='time')

    arg_parser.add_argument('-c', '--clobber',
                            help='Clobber existing sensor defintions',
                            action='store_true')

    arg_parser.add_argument('-x', '--debug',
                            help='Check configuration and create NetCDF file writer, but does not process any files',
                            action='store_true')

    arg_parser.add_argument('-l', '--loglevel',
                            help='Verbosity level',
                            type=str,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            default='info')

    parsed_args = arg_parser.parse_args()

    # print(parsed_args)
    # sys.exit(13)

    sys.exit(main(parsed_args))