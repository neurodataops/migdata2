"""
snowflake_utils.py — Shared Snowflake utilities
================================================
Helper functions used by both the Streamlit app and the FastAPI backend
when working with Snowflake credentials.
"""

import re

# Connection timeout constants — keep these in one place so all callers
# are consistent.  Error 250001 ("Could not connect to Snowflake backend")
# is often caused by a firewall that silently drops packets; setting an
# explicit socket/network timeout makes the connector surface the error
# quickly instead of hanging indefinitely.
_LOGIN_TIMEOUT_SECONDS = 30   # time allowed for the login exchange
_NETWORK_TIMEOUT_SECONDS = 60  # max time for any single network operation


def normalize_snowflake_account(account: str) -> str:
    """Normalize a Snowflake account identifier.

    Strips common mistakes that cause connection error 250001:
    - Leading ``https://`` or ``http://`` scheme
    - Trailing ``.snowflakecomputing.com`` suffix
    - Leading/trailing whitespace and extra slashes

    Examples::

        normalize_snowflake_account("xy12345.snowflakecomputing.com")
        # -> "xy12345"

        normalize_snowflake_account("https://myorg-myaccount.snowflakecomputing.com/")
        # -> "myorg-myaccount"

        normalize_snowflake_account("myorg-myaccount")
        # -> "myorg-myaccount"
    """
    account = account.strip()
    # Remove URL scheme if accidentally included
    for prefix in ("https://", "http://"):
        if account.lower().startswith(prefix):
            account = account[len(prefix):]
    # Strip any leading slashes that may remain after scheme removal
    account = account.lstrip("/")
    # Strip trailing slashes
    account = account.rstrip("/")
    # Remove .snowflakecomputing.com suffix if present
    suffix = ".snowflakecomputing.com"
    if account.lower().endswith(suffix):
        account = account[: -len(suffix)]
    return account


def validate_snowflake_account(account: str) -> list[str]:
    """Return a list of human-readable warnings about the account identifier.

    The connector will still be called even when warnings are present, but
    surfacing them early helps users identify misconfiguration before they
    see error 250001.

    Returns an empty list if the identifier looks fine.
    """
    warnings: list[str] = []
    raw = account.strip()
    if not raw:
        warnings.append("Account identifier is empty.")
        return warnings

    normalized = normalize_snowflake_account(raw)

    if raw != normalized:
        warnings.append(
            f"Account identifier was normalized to '{normalized}'. "
            "Do not include 'https://' or '.snowflakecomputing.com' in the account field."
        )

    if not normalized:
        warnings.append("Account identifier is empty after normalization.")
        return warnings

    # A valid account identifier contains only alphanumeric characters,
    # hyphens, underscores, and dots (for region-qualified locators like
    # xy12345.us-east-1.aws).  A URL leftover (e.g. 'myorg.my-account/path')
    # would contain a slash.
    if re.search(r"[/?#]", normalized):
        warnings.append(
            f"Account identifier '{normalized}' contains unexpected characters. "
            "Expected format: 'myorg-myaccount' or 'xy12345.us-east-1.aws'."
        )

    return warnings


def build_snowflake_connect_kwargs(
    *,
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    role: str,
) -> dict:
    """Return a dict of kwargs for ``snowflake.connector.connect``.

    Centralises the connection parameters so all callers (API endpoint,
    schema fetcher, adapter ``_connect``) use identical settings.  The
    ``socket_timeout`` and ``network_timeout`` values ensure that a
    firewall-blocked connection fails quickly with an actionable error
    message rather than hanging for minutes.
    """
    return {
        "account": normalize_snowflake_account(account),
        "user": user,
        "password": password,
        "warehouse": warehouse,
        "database": database,
        "role": role,
        "login_timeout": _LOGIN_TIMEOUT_SECONDS,
        "network_timeout": _NETWORK_TIMEOUT_SECONDS,
        "socket_timeout": _NETWORK_TIMEOUT_SECONDS,
    }
