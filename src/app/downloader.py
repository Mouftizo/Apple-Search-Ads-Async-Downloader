import datetime
import aiohttp
import asyncio
from src.config import config
import ssl
import pathlib
from typing import Callable
from src.config.api_definitions import modify_report
from src.helpers.helpers import apple_search_csv_writer, get_apple_data, get_apple_campaign_data, \
    generate_date_range
from src.utils.async_utils import write_files
from functools import partial
import sys

H = {"Authorization": "orgId={org_id}".format(org_id=config.account_id())}
C = config.cert_dir()


async def main():
    # prepare constants
    sslcontext = ssl.create_default_context(cafile=None)
    sslcontext.load_cert_chain(certfile=C + "Admin_API_access.pem", keyfile=C + "Admin_API_access.key")
    connector = aiohttp.TCPConnector(limit=0, ssl=sslcontext)  # reduce limit to avoid over-calling the api

    async with aiohttp.ClientSession(connector=connector) as context:
        apple_file_writer: Callable = partial(write_files, session=context, writer=apple_search_csv_writer, headers=H)

        tasks: list = []  # prepare tasks list

        # PREPARE CAMPAIGN REPORTS #
        days: list = generate_date_range(config.first_date())
        for day in days:
            report_fmt: dict = modify_report(day, day)  # get report format
            path: pathlib.Path = config.data_dir() / day.strftime('%Y/%m/%d') / 'applesearch_campaign_report.csv'
            # skip file if already exists
            if path.exists() and (datetime.datetime.utcnow().date() - day).days > config.update_report_interval():
                print(f'Skipping {path}, already exists')
                continue

            if not path.parent.exists():
                path.parent.mkdir(parents=True)

            # add tasks
            tasks.append(
                apple_file_writer(file=path,
                                  url="https://api.searchads.apple.com/api/v3/reports/campaigns",
                                  method="POST",
                                  first_col=day.strftime('%Y%m%d'),
                                  data_func=get_apple_data,
                                  json=report_fmt)
            )

        # PREPARE CAMPAIGN DATA #
        path: pathlib.Path = config.data_dir() / 'applesearch_campaigns.csv'
        # add task
        tasks.append(
            apple_file_writer(file=path,
                              url="https://api.searchads.apple.com/api/v3/campaigns",
                              method="GET",
                              data_func=get_apple_campaign_data,
                              first_col=datetime.datetime.utcnow().date().strftime('%Y%m%d'))
        )
        await asyncio.gather(*tasks)


def download_data():
    assert sys.version_info >= (3, 7), "Requires Python 3.7+."
    asyncio.run(main())
