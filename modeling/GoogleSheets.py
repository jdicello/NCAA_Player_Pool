# -*- coding: utf-8 -*-
"""
Google Sheets integration for NCAA Player Pool.

Authentication strategy (tried in order):
1. Service account JSON at SERVICE_ACCOUNT_FILE — used by GitHub Actions (headless).
2. Cached OAuth2 token at TOKEN_PATH — used for interactive local runs.
3. OAuth2 browser flow — first-time local auth; requires client_secret.json.
"""

import logging
import os

import googleapiclient.discovery
import googleapiclient.errors
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

_HERE = os.path.dirname(__file__)

# Service account JSON written by GitHub Actions (or placed manually for headless use)
SERVICE_ACCOUNT_FILE = os.path.join(_HERE, "NCAAPlayerPool-09b6773d89ec.json")

# client_secret.json for interactive OAuth (local use only)
CLIENT_SECRET_FILE = os.path.join(_HERE, "client_secret.json")

# Cached OAuth2 token (created automatically on first interactive auth)
TOKEN_PATH = os.path.join(
    os.path.expanduser("~"), ".credentials", "ncaa_pool_token.json"
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _get_credentials():
    """
    Return valid Google credentials using the best available method.

    Priority:
      1. Service account JSON (headless / GitHub Actions)
      2. Cached OAuth2 user token (interactive local run, already authorized)
      3. OAuth2 browser flow (first-time local auth)
    """
    # 1. Service account — preferred for headless environments
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        logger.debug("Using service account credentials from %s", SERVICE_ACCOUNT_FILE)
        return service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )

    # 2 & 3. OAuth2 flow for interactive local use
    creds = None

    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
            logger.debug("Loaded OAuth2 token from %s", TOKEN_PATH)
        except Exception as exc:
            logger.warning("Could not load token from %s: %s", TOKEN_PATH, exc)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            logger.debug("Refreshed OAuth2 credentials successfully")
        except Exception as exc:
            logger.warning("Token refresh failed, re-authorizing: %s", exc)
            creds = None

    if not creds or not creds.valid:
        logger.info(
            "No valid credentials found — opening browser for authorization. "
            "Follow the prompts in your browser."
        )
        flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
        logger.info("Authorization complete")

    # Persist the (possibly refreshed) token
    os.makedirs(os.path.dirname(TOKEN_PATH), exist_ok=True)
    with open(TOKEN_PATH, "w") as token_file:
        token_file.write(creds.to_json())
    logger.debug("Saved OAuth2 credentials to %s", TOKEN_PATH)

    return creds


def _build_service():
    """
    Build and return an authenticated Google Sheets API v4 service object.
    """
    creds = _get_credentials()
    return googleapiclient.discovery.build("sheets", "v4", credentials=creds, static_discovery=False)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def writeGoogleSheet(output: list, spreadsheetId: str, rangeName: str) -> dict:
    """
    Write a 2-D list to a Google Sheet range.

    Parameters
    ----------
    output : list[list]
        Row-major data.  Each inner list becomes one row in the spreadsheet.
    spreadsheetId : str
        The target Google Spreadsheet ID (from the sheet URL).
    rangeName : str
        A1-notation range, e.g. ``'All Players!A2'``.

    Returns
    -------
    dict
        The API response from the spreadsheets.values.update call.

    Raises
    ------
    googleapiclient.errors.HttpError
        If the Sheets API returns a 4xx/5xx response.
    """
    logger.info(
        "Writing %d rows to spreadsheet %s range %s",
        len(output),
        spreadsheetId,
        rangeName,
    )
    service = _build_service()
    try:
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheetId,
                valueInputOption="USER_ENTERED",
                range=rangeName,
                body={
                    "majorDimension": "ROWS",
                    "range": rangeName,
                    "values": output,
                },
            )
            .execute()
        )
    except googleapiclient.errors.HttpError as exc:
        logger.error(
            "Google Sheets API error writing to %s %s: %s",
            spreadsheetId,
            rangeName,
            exc,
        )
        raise

    logger.info(
        "Wrote %s cells to %s", result.get("updatedCells", "?"), rangeName
    )
    return result


def readGoogleSheet(spreadsheetId: str, rangeName: str) -> list:
    """
    Read values from a Google Sheet range.

    Parameters
    ----------
    spreadsheetId : str
        The target Google Spreadsheet ID (from the sheet URL).
    rangeName : str
        A1-notation range, e.g. ``"'All Players'!A:A"``.

    Returns
    -------
    list[list]
        Row-major data.  Each inner list is one row.  Empty if the range
        has no values.

    Raises
    ------
    googleapiclient.errors.HttpError
        If the Sheets API returns a 4xx/5xx response.
    """
    logger.info("Reading from spreadsheet %s range %s", spreadsheetId, rangeName)
    service = _build_service()
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheetId, range=rangeName)
            .execute()
        )
    except googleapiclient.errors.HttpError as exc:
        logger.error(
            "Google Sheets API error reading %s %s: %s",
            spreadsheetId,
            rangeName,
            exc,
        )
        raise

    values = result.get("values", [])
    logger.info("Read %d rows from %s", len(values), rangeName)
    return values
