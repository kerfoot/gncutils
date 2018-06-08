#!/usr/bin/env python

import os
import sys
import logging
import argparse
import json
from netCDF4 import Dataset


def main(args):
    # Set up logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    nc_file = args.nc_file

    if not os.path.isfile(nc_file):
        logging.error('File does not exist: {:s}'.format(nc_file))
        return 1

    try:
        nci = Dataset(nc_file, 'r')
    except IOError as e:
        logging.error('Error opening {:s}: {:}'.format(nc_file, e))
        return 1

    nc_json = {'global_attributes' : {}, 'variables': {}}
    global_atts = nci.ncattrs()
    for global_att in global_atts:
        att_val = nci.getncattr(global_att)
        num = nptype2number(att_val)
        if num:
            nc_json['global_attributes'][global_att] = num
        else:
            nc_json['global_attributes'][global_att] = att_val

    nc_dims = nci.dimensions.keys()
    for var_name in nci.variables:

        v = nci.variables[var_name]
        var_desc = {'nc_var_name': var_name,
                    'type': v.datatype.str,
                    'dimension': v.dimensions,
                    'attrs': {},
                    'is_dimension': False}

        if v.name in nc_dims:
            var_desc['is_dimension'] = True,
            var_desc['dimension_length'] = None

        for att in v.ncattrs():
            att_val = v.getncattr(att)
            num = nptype2number(att_val)
            if num is not None:
                var_desc['attrs'][att] = num
            else:
                var_desc['attrs'][att] = att_val

        nc_json['variables'][var_name] = var_desc

    sys.stdout.write('{:s}\n'.format(json.dumps(nc_json, indent=4, sort_keys=True)))

    return 0


def nptype2number(nptype):

    props = dir(nptype)
    if 'dtype' in props:
        if str(nptype.dtype).startswith('int'):
            return int(nptype)
        else:
            return float(nptype)
    else:
        return None


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('nc_file',
                            help='NetCDF file to parse to json')

    arg_parser.add_argument('-l', '--loglevel',
                            help='Verbosity level',
                            type=str,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            default='info')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))

