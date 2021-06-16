#!/usr/bin/env python

import logging
import sys
import argparse
import os
import yaml
from gncutils.dacftp import DacFtpClient


def main(args):
    """FTP the list of NetCDF files to the DAC ftp server for the specified dataset_id"""

    dataset_id = args.dataset_id
    nc_files = args.nc_files
    user = args.username
    url = args.url
    config_file = args.config_file
    pw = args.password
    debug_level = args.debug_level
    debug = args.debug

    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    if config_file:
        if not os.path.isfile(config_file):
            logging.error('Invalid configuration file specified: {:}'.format(config_file))
            return 1
        with open(config_file, 'r') as fid:
            cfg_params = yaml.safe_load(fid)
            user = cfg_params.get('username', None)
            pw = cfg_params.get('password', None)
            url = cfg_params.get('url', None)

    if not url:
        logging.error('No FTP site specified')
        return 1
    if not user:
        logging.error('No username specified')
        return 1
    if not pw:
        logging.error('No password specified')
        return 1

    ftp = DacFtpClient(url=url, user=user, pw=pw)
    logging.info(ftp.client.getwelcome())
    ftp.debug_level = debug_level

    # Make sure the dataset exists
    if dataset_id not in ftp.datasets.index:
        logging.error('Dataset {:} does not exist on the remote server'.format(dataset_id))
        return 1

    if debug:
        logging.info('FTP connection: {:}'.format(ftp))
        logging.info('Dataset ID: {:}'.format(dataset_id))
        for nc in nc_files:
            if not nc.endswith('.nc'):
                continue
            sys.stdout.write('{:}\n'.format(nc))
        return 0

    transferred_files = ftp.mput_nc_files(dataset_id, nc_files)

    for f in transferred_files:
        sys.stdout.write('{:}\n'.format(f))

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('dataset_id',
                            help='Registered DAC deployment dataset id',
                            type=str)

    arg_parser.add_argument('nc_files',
                            nargs='+',
                            help='NetCDF files to upload')

    arg_parser.add_argument('-c', '--config_file',
                            help='YAML configuration file specifying FTP url and credentials')

    arg_parser.add_argument('--url',
                            help='FTP url',
                            default='54.89.120.221')

    arg_parser.add_argument('-u', '--user',
                            dest='username',
                            help='User name',
                            default='rutgers')

    arg_parser.add_argument('-p', '--password',
                            help='Password')

    arg_parser.add_argument('--debug_level',
                            help='FTP server debug verbosity level',
                            default=0,
                            choices=[0, 1, 2])

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