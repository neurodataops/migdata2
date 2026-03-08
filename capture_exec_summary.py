"""
Capture the Executive Summary tab with Snowflake source selected,
then compose it into a laptop mockup branded 'MigData by Happiest Minds'.
"""

import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from PIL import Image, ImageDraw, ImageFont

OUTPUT_DIR = Path(r"C:\dev\data-migration\screenshots")
OUTPUT_DIR.mkdir(exist_ok=True)

SCREENSHOT_PATH = OUTPUT_DIR / "exec_summary_snowflake.png"
FINAL_PATH = OUTPUT_DIR / "migdata_laptop_mockup.png"


def capture_screenshot():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--force-device-scale-factor=1")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=opts)
    driver.set_window_size(1920, 1080)

    try:
        driver.get("http://localhost:8501")
        time.sleep(6)

        # ── Step 1: Login ──
        print("Step 1: Logging in...")
        inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='text'], input[aria-label='Username']")
        if inputs:
            inputs[0].clear()
            inputs[0].send_keys("admin")

        pw_inputs = driver.find_elements(By.CSS_SELECTOR, "input[type='password']")
        if pw_inputs:
            pw_inputs[0].clear()
            pw_inputs[0].send_keys("admin@123")

        time.sleep(1)
        login_btns = driver.find_elements(By.XPATH, "//button[contains(.,'Log In')]")
        if login_btns:
            login_btns[0].click()
            print("  Logged in")

        time.sleep(5)

        # ── Step 2: Connection Page — select Snowflake + Mock ──
        print("Step 2: Connection page...")
        page_text = driver.find_element(By.TAG_NAME, "body").text

        if "Connect" in page_text or "Use Mock" in page_text:
            # Check "Use Mock Data"
            mock_cbs = driver.find_elements(By.XPATH,
                "//div[contains(@data-testid,'stCheckbox')]//label[contains(.,'Mock')]"
            )
            if not mock_cbs:
                mock_cbs = driver.find_elements(By.XPATH, "//label[contains(.,'Use Mock Data')]")
            if mock_cbs:
                mock_cbs[0].click()
                print("  Checked Use Mock Data")
                time.sleep(2)

            # Select Snowflake
            sf_els = driver.find_elements(By.XPATH,
                "//label[.//p[contains(text(),'Snowflake')]] | //div[contains(@role,'radiogroup')]//label[contains(.,'Snowflake')]"
            )
            if sf_els:
                sf_els[0].click()
                print("  Selected Snowflake")
                time.sleep(2)

            # Test Source Connection
            test_btns = driver.find_elements(By.XPATH, "//button[contains(.,'Test Source')]")
            if test_btns:
                test_btns[0].click()
                print("  Testing source connection...")
                time.sleep(4)

            # Proceed to Dashboard
            proceed_btns = driver.find_elements(By.XPATH, "//button[contains(.,'Proceed')]")
            if proceed_btns:
                proceed_btns[0].click()
                print("  Proceeding to Dashboard...")

        # ── Step 3: Wait for pipeline to finish ──
        print("Step 3: Waiting for pipeline to complete...")

        # The pipeline shows "Running Full Demo Pipeline" while active.
        # Wait up to 120 seconds for it to finish (tabs become visible).
        max_wait = 120
        poll_interval = 5
        elapsed = 0
        pipeline_done = False

        while elapsed < max_wait:
            time.sleep(poll_interval)
            elapsed += poll_interval
            body = driver.find_element(By.TAG_NAME, "body").text
            # Pipeline is done when "Executive Summary" tab text appears
            # AND "Running Full Demo Pipeline" is no longer present
            if "Executive" in body and "Running Full Demo Pipeline" not in body:
                pipeline_done = True
                print(f"  Pipeline completed after ~{elapsed}s")
                break
            # Also check if Executive Summary tab button exists
            tabs = driver.find_elements(By.XPATH, "//button[contains(.,'Executive Summary')]")
            if tabs and "Running Full Demo Pipeline" not in body:
                pipeline_done = True
                print(f"  Pipeline completed after ~{elapsed}s")
                break
            print(f"  Still running... ({elapsed}s)")

        if not pipeline_done:
            # If still running, wait extra and reload
            print("  Pipeline may still be running, waiting more...")
            time.sleep(15)

        time.sleep(3)

        # ── Step 4: Ensure Executive Summary tab is active ──
        print("Step 4: Selecting Executive Summary tab...")

        # Ensure Snowflake is selected in sidebar
        sidebar_sf = driver.find_elements(By.XPATH,
            "//div[contains(@data-testid,'stSidebar')]//label[.//p[contains(text(),'Snowflake')]]"
        )
        if sidebar_sf:
            # Check if it's already selected
            sidebar_text = driver.find_element(By.CSS_SELECTOR, "[data-testid='stSidebar']").text
            if "Snowflake" in sidebar_text:
                print("  Snowflake already selected in sidebar")

        # Click Executive Summary tab
        exec_tabs = driver.find_elements(By.XPATH, "//button[contains(.,'Executive Summary')]")
        if exec_tabs:
            exec_tabs[0].click()
            print("  Clicked Executive Summary tab")
            time.sleep(4)

        # Scroll to top to capture the summary from the beginning
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)

        # ── Step 5: Capture screenshot ──
        print("Step 5: Capturing screenshot...")

        # Get total page height for a full-page capture
        total_height = driver.execute_script("return document.body.scrollHeight")
        # Capture a good portion including KPIs, charts, and progress overview
        capture_height = min(total_height, 1800)
        driver.set_window_size(1920, capture_height)
        time.sleep(3)

        # Scroll to top again after resize
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)

        driver.save_screenshot(str(SCREENSHOT_PATH))
        print(f"  Screenshot saved: {SCREENSHOT_PATH}")

    finally:
        driver.quit()


def create_laptop_mockup():
    screenshot = Image.open(SCREENSHOT_PATH)
    sw, sh = screenshot.size

    # Crop to a good aspect ratio for laptop display (roughly 16:10)
    target_ratio = 16 / 10
    current_ratio = sw / sh
    if current_ratio < target_ratio:
        # Too tall — crop height
        new_h = int(sw / target_ratio)
        screenshot = screenshot.crop((0, 0, sw, min(new_h, sh)))
    sw, sh = screenshot.size

    # Screen area inside the laptop
    screen_w = 1440
    screen_h = int(screen_w * sh / sw)
    if screen_h > 900:
        screen_h = 900
    screenshot_resized = screenshot.resize((screen_w, screen_h), Image.LANCZOS)

    # Laptop dimensions
    bezel = 18
    frame_w = screen_w + bezel * 2
    frame_h = screen_h + bezel * 2
    base_height = 44
    base_extend = 90

    total_w = frame_w + base_extend * 2
    top_margin = 90
    bottom_margin = 75
    total_h = top_margin + frame_h + base_height + bottom_margin

    # Canvas
    canvas = Image.new("RGB", (total_w, total_h), (12, 12, 28))
    draw = ImageDraw.Draw(canvas)

    # Subtle radial background glow
    cx, cy = total_w // 2, total_h // 2
    for r in range(500, 0, -3):
        f = r / 500
        c = int(14 + (1 - f) * 12)
        draw.ellipse([cx - r * 2, cy - r, cx + r * 2, cy + r], fill=(c, c, c + 12))

    lid_x = (total_w - frame_w) // 2
    lid_y = top_margin

    # Outer bezel
    draw.rounded_rectangle(
        [lid_x, lid_y, lid_x + frame_w, lid_y + frame_h],
        radius=14, fill=(55, 55, 65)
    )
    # Inner bezel shadow
    draw.rounded_rectangle(
        [lid_x + 3, lid_y + 3, lid_x + frame_w - 3, lid_y + frame_h - 3],
        radius=12, fill=(40, 40, 50)
    )
    # Screen border
    draw.rounded_rectangle(
        [lid_x + bezel - 1, lid_y + bezel - 1,
         lid_x + bezel + screen_w + 1, lid_y + bezel + screen_h + 1],
        radius=3, fill=(10, 10, 15)
    )

    # Paste screenshot
    canvas.paste(screenshot_resized, (lid_x + bezel, lid_y + bezel))

    # Webcam
    cam_x = lid_x + frame_w // 2
    cam_y = lid_y + bezel // 2
    draw.ellipse([cam_x - 4, cam_y - 4, cam_x + 4, cam_y + 4], fill=(35, 35, 45))
    draw.ellipse([cam_x - 2, cam_y - 2, cam_x + 2, cam_y + 2], fill=(55, 55, 70))

    # Hinge
    hinge_y = lid_y + frame_h
    draw.rounded_rectangle(
        [lid_x + 50, hinge_y, lid_x + frame_w - 50, hinge_y + 6],
        radius=3, fill=(65, 65, 75)
    )

    # Base (trapezoid)
    base_y = hinge_y + 6
    draw.polygon([
        (lid_x - 15, base_y),
        (lid_x + frame_w + 15, base_y),
        (lid_x + frame_w + base_extend, base_y + base_height),
        (lid_x - base_extend, base_y + base_height)
    ], fill=(52, 52, 62))

    # Trackpad
    tp_w, tp_h = 180, 16
    tp_x = total_w // 2 - tp_w // 2
    tp_y = base_y + (base_height - tp_h) // 2 + 2
    draw.rounded_rectangle([tp_x, tp_y, tp_x + tp_w, tp_y + tp_h], radius=5, fill=(62, 62, 74))

    # Bottom edge
    draw.line(
        [(lid_x - base_extend, base_y + base_height),
         (lid_x + frame_w + base_extend, base_y + base_height)],
        fill=(75, 75, 90), width=2
    )

    # ── Branding ──
    def get_font(size, bold=False):
        paths = ([r"C:\Windows\Fonts\segoeuib.ttf", r"C:\Windows\Fonts\arialbd.ttf"] if bold
                 else [r"C:\Windows\Fonts\segoeui.ttf", r"C:\Windows\Fonts\arial.ttf"])
        for fp in paths:
            try:
                return ImageFont.truetype(fp, size)
            except (OSError, IOError):
                continue
        return ImageFont.load_default()

    title_font = get_font(44, bold=True)
    by_font = get_font(24)
    tag_font = get_font(15)

    title_text = "MigData"
    by_text = "by Happiest Minds"
    bb_t = draw.textbbox((0, 0), title_text, font=title_font)
    tw = bb_t[2] - bb_t[0]
    bb_b = draw.textbbox((0, 0), by_text, font=by_font)
    bw = bb_b[2] - bb_b[0]

    sx = (total_w - tw - 14 - bw) // 2
    draw.text((sx, 22), title_text, fill=(138, 92, 246), font=title_font)
    draw.text((sx + tw + 14, 36), by_text, fill=(210, 210, 225), font=by_font)

    tagline = "Snowflake \u2192 Databricks  \u2502  Executive Migration Summary  \u2502  Data Migration Intelligence Platform"
    bb_tag = draw.textbbox((0, 0), tagline, font=tag_font)
    draw.text(((total_w - bb_tag[2] + bb_tag[0]) // 2, base_y + base_height + 16),
              tagline, fill=(130, 130, 155), font=tag_font)

    # Subtle glow
    for i in range(20):
        y = base_y + base_height + 48 + i
        spread = max(0, 220 - i * 6)
        x1, x2 = total_w // 2 - spread, total_w // 2 + spread
        if x1 < x2 and y < total_h:
            c = max(0, 18 - i)
            draw.line([(x1, y), (x2, y)], fill=(95 + c, 65 + c, 195 + c), width=1)

    canvas.save(str(FINAL_PATH), "PNG")
    print(f"Laptop mockup saved: {FINAL_PATH}")
    print(f"Image size: {canvas.size}")


if __name__ == "__main__":
    print("=== Step 1: Capturing Snowflake Executive Summary ===")
    capture_screenshot()
    print("\n=== Step 2: Creating laptop mockup ===")
    create_laptop_mockup()
    print("\nDone!")
