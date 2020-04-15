#!/usr/bin/env python

import argparse
import os
import logging
import json
import sys
import re
import yaml


def main(args):
    """Parse one or more Slocum dinkum binary data header files and write corresponding sensor definitions as valid
    YAML files"""

    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    status = 0

    cac_files = args.cac_files
    dimension = args.dimension
    clobber = args.clobber

    defined_sensors = []
    for cac_file in cac_files:

        if not os.path.isfile(cac_file):
            logging.error('Invalid .cac file specified: {:s}'.format(cac_file))
            continue

        if not cac_file.endswith('.cac'):
            logging.error('Invalid .cac file specified: {:s}'.format(cac_file))
            continue

        logging.info('Parsing header file: {:}'.format(cac_file))

        # Read args.cac_file
        try:
            with open(cac_file, 'r') as fid:
                cac_contents = fid.readlines()
        except (IOError, UnicodeDecodeError) as e:
            logging.error('{}'.format(e))
            continue

        def_regex = re.compile(r'^s:\s+[TF]\s+\d+\s+-?\d+\s+(\d+)\s+(\w+)\s+(.*)$')
        line_count = 0
        for cac_sensor in cac_contents:

            line_count += 1

            cac_sensor = cac_sensor.strip()

            match = def_regex.search(cac_sensor)

            if not match:
                logging.warning('{:}: Invalid cac sensor line #{:}: {:s}'.format(cac_file, line_count, cac_sensor))
                continue

            sensor_name = match.groups()[1]

            defined_sensors.append(sensor_name)

            out_file = os.path.join(args.outputdir, '{:}.yml'.format(sensor_name))
            if args.json:
                out_file = os.path.join(args.outputdir, '{:}.json'.format(sensor_name))

            if os.path.isfile(out_file):
                if not clobber:
                    logging.debug('Skipping existing sensor definition: {:}'.format(out_file))
                    continue
                else:
                    logging.warning('Clobbering existing sensor definition: {:}'.format(out_file))

            dtype = 'f8'
            num_bytes = int(match.groups()[0])
            if num_bytes == 1:
                dtype = 'i1'
            if num_bytes == 2:
                dtype = 'i2'
            elif num_bytes == 4:
                dtype = 'f4'

            units = match.groups()[2]
            sensor_def = {sensor_name: {
                'attrs': {
                    'sensor': sensor_name,
                    'units': units,
                    'long_name': sensor_name,
                    'comment': 'Native glider sensor name',
                    'processing_level': 0
                },
                'dimension': dimension,
                'nc_var_name': sensor_name,
                'type': dtype}
            }

            try:
                with open(out_file, 'w') as fid:
                    if args.json:
                        json.dump(sensor_def, fid, indent=4, sort_keys=True)
                    else:
                        yaml.safe_dump(sensor_def, fid, default_flow_style=False)
            except IOError as e:
                logging.error('{:}'.format(e))

    logging.info('{:0.0f} sensor definitions written'.format(len(set(defined_sensors))))

    return status


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('cac_files',
                            nargs='*',
                            help='Slocum .cac file to parse')

    arg_parser.add_argument('-o', '--outputdir',
                            help='Location to write individual sensor definition json files',
                            default=os.path.realpath(os.curdir))

    arg_parser.add_argument('-d', '--dimension',
                            help='Dimension name',
                            choices=['time', 'obs'],
                            default='time')

    arg_parser.add_argument('-c', '--clobber',
                            help='Clobber existing sensor defintions',
                            action='store_true')

    arg_parser.add_argument('-j', '--json',
                            help='Export definitions as json',
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
