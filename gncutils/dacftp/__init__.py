import ftplib
from gncutils.dacftp.utils import ftp_listing_to_dataframe
import pandas as pd
import os
import datetime
import logging
from dateutil import parser


class DacFtpClient(object):

    def __init__(self, url='54.89.120.221', user='rutgers', pw='1ioo$man9', debug_level=0):
        self._logger = logging.getLogger(__name__)
        self._user = user
        self._pw = pw
        self._url = url
        self._debug_level = debug_level
        self._datasets = pd.DataFrame([])
        # connect
        self._client = ftplib.FTP(url, user, pw, timeout=300)
        self._home = self._client.pwd()

        # Fetch available dataset files
        self.get_datasets()

    @property
    def client(self):
        return self._client

    @property
    def username(self):
        return self._user

    @property
    def password(self):
        return self._pw

    @property
    def url(self):
        return self._url

    @property
    def datasets(self):
        return self._datasets

    @property
    def debug_level(self):
        return self._debug_level

    @debug_level.setter
    def debug_level(self, level):
        if level < 0 or level > 2:
            self._logger.warning('Invalid FTP debug level {:}'.format(level))
            return
        self._client.set_debuglevel(level)

    def get_datasets(self):

        self._logger.info('Fetching available datasets for user {:}'.format(self._user))

        # Get the directory listing
        listing = []
        self._client.retrlines('LIST', listing.append)

        # Create the datasets dataframe
        self._datasets = ftp_listing_to_dataframe(listing)

    def get_dataset_file_listing(self, dataset_id):
        if dataset_id not in self._datasets.index:
            self._logger.error('Remote dataset does not exist: {:}'.format(dataset_id))
            return pd.DataFrame([])

        files = []
        self._client.dir('{:}'.format(os.path.join(self._home, dataset_id)), files.append)

        if not files:
            return pd.DataFrame([])

        file_listing = ftp_listing_to_dataframe(files)
        file_listing['ext'] = [os.path.splitext(f)[1] for f in file_listing.index]

        return file_listing

    def mput_nc_files(self, dataset_id, nc_files):

        uploaded_files = []
        if not nc_files:
            self._logger.error('No files specified')
            return uploaded_files

        if dataset_id not in self._datasets.index:
            self._logger.error('Remote dataset does not exist: {:}'.format(dataset_id))
            return uploaded_files

        remote_path = os.path.join(self._home, dataset_id)
        self._logger.info('Remote FTP destination: {:}'.format(remote_path))

        for nc_file in nc_files:

            if not os.path.isfile(nc_file):
                self._logger.warning('Local file {:} does not exist'.format(nc_file))
                continue

            if not nc_file.endswith('.nc'):
                self._logger.warning('Local file {:} does not appear to be a NetCDF file'.format(nc_file))
                continue

            self._logger.info('Uploading {:}'.format(nc_file))
            try:
                with open(nc_file, 'br') as fid:
                    self._client.storbinary(
                        'STOR {:}'.format(os.path.join(remote_path, os.path.basename(nc_file))), fid)
                    uploaded_files.append(nc_file)
            except ftplib.all_errors as e:
                self._logger.error(e)

        return uploaded_files

    @staticmethod
    def filter_listing_to_nc(file_listing):
        return file_listing.loc[file_listing.ext == '.nc']

    def __repr__(self):
        return '<DacFtpClient(url={:}, user={:}, num_datasets={:}, debug_level={:})>'.format(self._client.host,
                                                                                             self._user,
                                                                                             self._datasets.shape[0],
                                                                                             self._debug_level)
