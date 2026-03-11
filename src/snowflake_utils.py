"""
snowflake_utils.py — Shared Snowflake utilities
================================================
Helper functions used by both the Streamlit app and the FastAPI backend
when working with Snowflake credentials.
"""


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
