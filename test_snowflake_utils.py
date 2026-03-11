"""
Unit tests for src/snowflake_utils.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from snowflake_utils import (
    normalize_snowflake_account,
    validate_snowflake_account,
    build_snowflake_connect_kwargs,
    _LOGIN_TIMEOUT_SECONDS,
    _NETWORK_TIMEOUT_SECONDS,
)


# ── normalize_snowflake_account ──────────────────────────────────────────────

def test_normalize_already_clean():
    assert normalize_snowflake_account("myorg-myaccount") == "myorg-myaccount"


def test_normalize_strips_snowflakecomputing_suffix():
    assert normalize_snowflake_account("myorg.snowflakecomputing.com") == "myorg"


def test_normalize_strips_full_url_with_https():
    result = normalize_snowflake_account("https://myorg-myaccount.snowflakecomputing.com")
    assert result == "myorg-myaccount"


def test_normalize_strips_full_url_with_trailing_slash():
    result = normalize_snowflake_account("https://myorg-myaccount.snowflakecomputing.com/")
    assert result == "myorg-myaccount"


def test_normalize_strips_http_scheme():
    result = normalize_snowflake_account("http://xy12345.snowflakecomputing.com")
    assert result == "xy12345"


def test_normalize_strips_whitespace():
    assert normalize_snowflake_account("  myorg-myaccount  ") == "myorg-myaccount"


def test_normalize_region_qualified_locator():
    # Region-qualified locators must be preserved as-is
    assert (
        normalize_snowflake_account("xy12345.us-east-1.aws")
        == "xy12345.us-east-1.aws"
    )


def test_normalize_region_qualified_with_domain():
    result = normalize_snowflake_account("xy12345.us-east-1.aws.snowflakecomputing.com")
    assert result == "xy12345.us-east-1.aws"


def test_normalize_triple_slash_edge_case():
    # https:/// leaves two leading slashes after scheme removal
    result = normalize_snowflake_account("https:///account.snowflakecomputing.com")
    assert result == "account"


def test_normalize_mixed_case_suffix():
    result = normalize_snowflake_account("MyOrg-MyAccount.SnowflakeComputing.Com")
    assert result == "MyOrg-MyAccount"


# ── validate_snowflake_account ───────────────────────────────────────────────

def test_validate_clean_account_no_warnings():
    assert validate_snowflake_account("myorg-myaccount") == []


def test_validate_empty_account_gives_warning():
    warnings = validate_snowflake_account("")
    assert any("empty" in w.lower() for w in warnings)


def test_validate_whitespace_only_gives_warning():
    warnings = validate_snowflake_account("   ")
    assert any("empty" in w.lower() for w in warnings)


def test_validate_full_url_gives_warning():
    warnings = validate_snowflake_account("https://myorg.snowflakecomputing.com")
    assert len(warnings) > 0
    assert any("normalized" in w for w in warnings)


def test_validate_suffix_only_gives_warning():
    warnings = validate_snowflake_account("myaccount.snowflakecomputing.com")
    assert len(warnings) > 0


def test_validate_account_with_path_gives_warning():
    # URL remnant with a path segment
    warnings = validate_snowflake_account("myorg-myaccount/somepath")
    assert any("unexpected" in w.lower() for w in warnings)


# ── build_snowflake_connect_kwargs ────────────────────────────────────────────

def test_build_kwargs_normalizes_account():
    kwargs = build_snowflake_connect_kwargs(
        account="https://myorg.snowflakecomputing.com/",
        user="u",
        password="p",
        warehouse="wh",
        database="db",
        role="r",
    )
    assert kwargs["account"] == "myorg"


def test_build_kwargs_sets_timeouts():
    kwargs = build_snowflake_connect_kwargs(
        account="myorg",
        user="u",
        password="p",
        warehouse="wh",
        database="db",
        role="r",
    )
    assert kwargs["login_timeout"] == _LOGIN_TIMEOUT_SECONDS
    assert kwargs["network_timeout"] == _NETWORK_TIMEOUT_SECONDS
    assert kwargs["socket_timeout"] == _NETWORK_TIMEOUT_SECONDS


def test_build_kwargs_contains_required_fields():
    kwargs = build_snowflake_connect_kwargs(
        account="myorg",
        user="alice",
        password="secret",
        warehouse="COMPUTE_WH",
        database="ANALYTICS",
        role="SYSADMIN",
    )
    assert kwargs["user"] == "alice"
    assert kwargs["password"] == "secret"
    assert kwargs["warehouse"] == "COMPUTE_WH"
    assert kwargs["database"] == "ANALYTICS"
    assert kwargs["role"] == "SYSADMIN"


if __name__ == "__main__":
    # Allow running directly: python test_snowflake_utils.py
    import traceback
    tests = [
        (name, func) for name, func in globals().items()
        if name.startswith("test_") and callable(func)
    ]
    passed = failed = 0
    for name, func in tests:
        try:
            func()
            print(f"  ✓ {name}")
            passed += 1
        except Exception as exc:
            print(f"  ✗ {name}: {exc}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
