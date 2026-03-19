# -*- coding: utf-8 -*-
"""
Google Sheets integration for NCAA Player Pool.

Authentication uses OAuth2 with a client_secret.json file stored alongside
this script.  On the first run the browser will open for authorization; the
resulting token is cached at TOKEN_PATH and refreshed automatically on
subsequent runs.

Note: This module replaces the legacy oauth2client/httplib2 stack with the
modern google-auth / google-auth-oauthlib libraries.  If you previously
authorized via the old stack, delete the old token file at
~/.credentials/sheets.googleapis.com-python-quickstart.json and re-authorize.
"""

import logging
import os

import googleapiclient.discovery
import googleapiclient.errors
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# client_secret.json lives in the same directory as this script
CLIENT_SECRET_FILE = os.path.join(os.path.dirname(__file__), "client_secret.json")

# Cached OAuth2 token (created automatically on first auth)
TOKEN_PATH = os.path.join(
    os.path.expanduser("~"), ".credentials", "ncaa_pool_token.json"
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _get_credentials() -> Credentials:
    """
    Load cached OAuth2 credentials, refreshing or re-authorizing as needed.

    The token is stored at TOKEN_PATH.  If it does not exist or cannot be
    refreshed, the OAuth2 browser flow is launched to obtain a new token.

    Returns
    -------
    google.oauth2.credentials.Credentials
    """
    creds = None

    if os.path.exists(TOKEN_PATH):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
            logger.debug("Loaded credentials from %s", TOKEN_PATH)
        except Exception as exc:
            logger.warning("Could not load token from %s: %s", TOKEN_PATH, exc)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            logger.debug("Refreshed credentials successfully")
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
    logger.debug("Saved credentials to %s", TOKEN_PATH)

    return creds


def _build_service():
    """
    Build and return an authenticated Google Sheets API v4 service object.
    """
    creds = _get_credentials()
    return googleapiclient.discovery.build("sheets", "v4", credentials=creds)


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
