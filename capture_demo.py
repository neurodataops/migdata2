"""
capture_demo.py — Screenshot Automation for Demo
=================================================
Uses Playwright to open the Streamlit app and capture screenshots
of the key dashboard pages.

Prerequisites:
    pip install playwright
    playwright install chromium

Usage:
    python capture_demo.py

Captures:
    screenshots/demo_overview.png
    screenshots/demo_objects.png
    screenshots/demo_lineage.png
    screenshots/demo_conversion.png
    screenshots/demo_validation.png
    screenshots/demo_manual_work.png
"""

import subprocess
import sys
import time
from pathlib import Path

SCREENSHOTS_DIR = Path(__file__).resolve().parent / "screenshots"
STREAMLIT_URL = "http://localhost:8501"
WAIT_AFTER_NAV = 4000  # ms to wait for Streamlit to render


def ensure_playwright():
    """Check if Playwright is installed, prompt if not."""
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        return True
    except ImportError:
        print("Playwright is not installed.")
        print("Install with:")
        print("  pip install playwright")
        print("  playwright install chromium")
        return False


def capture_screenshots():
    """Launch browser, navigate through tabs, capture screenshots."""
    from playwright.sync_api import sync_playwright

    SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    tabs = [
        ("demo_overview.png", "📊 Overview", "Overview page"),
        ("demo_objects.png", "📋 Objects", "Objects explorer"),
        ("demo_lineage.png", "🔗 Lineage Graph", "Lineage graph"),
        ("demo_conversion.png", "🔀 SQL Comparison", "SQL comparison"),
        ("demo_validation.png", "✅ Validation", "Validation scorecards"),
        ("demo_manual_work.png", "📝 Manual Work", "Manual work panel"),
    ]

    print(f"Connecting to Streamlit at {STREAMLIT_URL}")
    print(f"Screenshots will be saved to: {SCREENSHOTS_DIR}")
    print()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            device_scale_factor=2,
        )
        page = context.new_page()

        # Navigate to app
        try:
            page.goto(STREAMLIT_URL, wait_until="networkidle", timeout=30000)
        except Exception as e:
            print(f"ERROR: Could not connect to {STREAMLIT_URL}")
            print(f"Make sure Streamlit is running: streamlit run app.py")
            print(f"Detail: {e}")
            browser.close()
            return

        # Wait for initial render
        page.wait_for_timeout(WAIT_AFTER_NAV)

        for filename, tab_text, label in tabs:
            print(f"  Capturing: {label}...", end=" ", flush=True)

            # Click the tab button
            try:
                tab_button = page.locator(f"button:has-text('{tab_text}')").first
                tab_button.click()
                page.wait_for_timeout(WAIT_AFTER_NAV)
            except Exception:
                # If tab not found, just capture current state
                pass

            # Scroll to top
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(500)

            # Capture full page
            path = SCREENSHOTS_DIR / filename
            page.screenshot(path=str(path), full_page=True)
            print(f"saved → {path}")

        browser.close()

    print()
    print(f"Done! {len(tabs)} screenshots saved to {SCREENSHOTS_DIR}")


def main():
    if not ensure_playwright():
        sys.exit(1)

    # Check if Streamlit is running
    print("Checking if Streamlit is running...")
    import urllib.request
    try:
        urllib.request.urlopen(STREAMLIT_URL, timeout=5)
        print("Streamlit is running.")
    except Exception:
        print(f"Streamlit not detected at {STREAMLIT_URL}.")
        print("Starting Streamlit in background...")
        proc = subprocess.Popen(
            [sys.executable, "-m", "streamlit", "run", "app.py",
             "--server.headless", "true", "--server.port", "8501"],
            cwd=str(Path(__file__).resolve().parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        print("Waiting for Streamlit to start...")
        time.sleep(8)

        # Verify it started
        try:
            urllib.request.urlopen(STREAMLIT_URL, timeout=10)
        except Exception:
            print("ERROR: Streamlit failed to start. Run manually first:")
            print("  streamlit run app.py")
            proc.terminate()
            sys.exit(1)

    capture_screenshots()


if __name__ == "__main__":
    main()
