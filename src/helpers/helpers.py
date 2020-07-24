import datetime
import itertools
from typing import IO, List
import csv


def generate_date_range(first_date: str):
    """"" Generates a date range between a starting date (as a string) and yesterday. """
    first_day_fmt: datetime.date = datetime.datetime.strptime(first_date, '%Y-%m-%d').date()
    yesterday: datetime.date = datetime.datetime.utcnow().date() - datetime.timedelta(days=1)
    date_generator = (yesterday - datetime.timedelta(days=i) for i in itertools.count())
    dates = itertools.islice(date_generator, (yesterday - first_day_fmt).days + 1)
    yield from dates


def get_apple_data(data: dict, first_col: str) -> List:
    """ Parses apple report data. """
    rows = data['data']["reportingDataResponse"]['row']
    cols = []
    for row in rows:
        r = (first_col,
             row['metadata']['campaignId'],
             row['metadata']['campaignName'],
             row['metadata']['campaignStatus'],
             row['total']['impressions'],
             row['total']['taps'],
             row['total']['installs'],
             row['total']['newDownloads'],
             row['total']['redownloads'],
             row['total']['avgCPA']['amount'],
             row['total']['avgCPT']['amount'],
             row['total']['localSpend']['amount'])
        cols.append(r)
    return cols


def get_apple_campaign_data(data: dict, first_col: str) -> List:
    """ Parses apple campaign data. """
    rows = data['data']
    cols = []
    for row in rows:
        r = (first_col,
             row['id'],
             row['name'],
             row['status'],
             row['deleted'])
        cols.append(r)
    return cols


def apple_search_csv_writer(file: IO, data: str) -> callable:
    """ Bulk-writes csv into file. """
    file_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    file_writer.writerows(data)
    return None