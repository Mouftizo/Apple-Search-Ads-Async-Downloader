import pathlib


def account_id():
    """
    The apple search ads account ID
    """
    return "123456"


def cert_dir():
    """
    The directory for the credential files
    """
    return str(pathlib.Path('../credentials').absolute()) + '/'


def data_dir():
    """
    The directory for the csv downloaded
    """
    return pathlib.Path('../foo')


def first_date():
    """
    The first date to download the reports.
    """
    return '1993-19-06'


def update_report_interval():
    """
    The intervals in days within which the reports shall be re-downloaded.
    """
    return 30
