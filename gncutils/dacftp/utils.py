import datetime
import pandas as pd
import logging
from dateutil import parser


def ftp_listing_to_dataframe(dir_listing):

    if not dir_listing:
        return pd.DataFrame()

    folders = [row.split() for row in dir_listing]

    this_year = datetime.datetime.utcnow().year

    rows = []
    for folder in folders:
        is_dir = False
        if folder[1] == 2:
            is_dir = True
        row = {'file_name': folder[8],
            'perms': folder[0],
            'month': folder[5],
            'day': folder[6],
            'year': None,
            'time': None,
            'is_dir': is_dir}
        tokens = folder[7].split(':')
        if len(tokens) > 2:
            logging.warning('Cannot determine ftp timestamp for token {:}'.format(folder[7]))
            row['month'] = 'Jan'
            row['day'] = '1'
            row['year'] = '1970'
            row['time'] = '00:00'
        elif len(tokens) == 1:
            row['year'] = folder[7]
            row['time'] = '00:00'
        else:
            row['year'] = this_year
            row['time'] = folder[7]

        rows.append(row)

    # Read it into a pandas DataFrame
    df = pd.DataFrame(rows).set_index('file_name')

    # Create the directory mtime from the date pieces
    mtimes = [parser.parse('{:} {:} {:} {:}'.format(row.month, row.day, row.year, row.time)) for _, row in df.iterrows()]
    df['mtime'] = mtimes
    df.drop(columns=['year', 'month', 'day', 'time'], inplace=True)

    return df
