from aiohttp import ClientResponse, ClientSession, ClientError, http_exceptions
from typing import Callable, Iterable, Union
from pathlib import Path
import logging
import sys

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True


async def fetch_data(url: str, method: str, session: ClientSession, **kwargs) -> dict:
    """
    Request wrapper that fetches data. Passes kwargs to session.
    Args:
        :param url: api endpoint used (eg. https://api.searchads.apple.com/api/v3/campaigns)
        :param method: HTTP method used (eg. GET, POST...etc.)
        :param session: aiohttp.ClientSession object. The main entry point for all client API operations.
    Kwargs:
        ___ : Includes other KWARGS that may be passed to a session (eg. Headers, SSL...etc.)
    Returns:
        Dict
     """
    response: ClientResponse = await session.request(url=url, method=method, **kwargs)
    response.raise_for_status()  # Raises an aiohttp.ClientResponseError if the response status is 400 or higher
    logger.info("Got response [%s] for request: %s", response.status, url)
    data: dict = await response.json()
    return data


async def write_files(file: Union[str, Path],
                      url: str,
                      method: str,
                      session: ClientSession,
                      writer: Callable,
                      data_func: Callable,
                      first_col='',
                      **kwargs) -> None:
    """
    File writer with side effects. Gets passed KWARGS from fetch_data.
    Args:
        :param file: name of file.
        :param url: api endpoint used (eg. https://someapi.itsname.com/api/version/endpoint).
        :param method: HTTP method used (eg. GET, POST...etc.).
        :param session: aiohttp.ClientSession object. The main entry point for all client API operations.
        :param writer: Function object that handles writing the data to an opened file.
        :param data_func: Function object that handles data cleaning.
        :param first_col: Optional argument that passes a first col value into the data_func.
    Kwargs:
         ___ : Includes other KWARGS that may be passed to a session (eg. Headers, SSL...etc.)
    Returns:
        None
    """
    try:
        preprocess_data: dict = await fetch_data(url=url, method=method, session=session, **kwargs)

    except (
            ClientError,
            http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return None
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return None

    else:
        if not preprocess_data:
            return None
        else:
            data: Iterable = data_func(preprocess_data, first_col)

    with open(file=file, mode='w') as f:
        writer(f, data)
        logger.info("Wrote results from URL: %s in %s", url, file)
