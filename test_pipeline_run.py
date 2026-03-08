"""
Direct test of pipeline with progress tracking
"""
import asyncio
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

from api.services.pipeline_runner import run_pipeline, get_status, subscribe


async def test_run():
    """Test running pipeline and receiving updates"""

    # Subscribe to updates
    queue = subscribe()

    # Start pipeline
    print("Starting pipeline...")
    task = asyncio.create_task(run_pipeline("snowflake", use_mock=False, selected_schemas=[]))

    # Listen for updates
    msg_count = 0
    try:
        while True:
            msg = await asyncio.wait_for(queue.get(), timeout=120)
            msg_count += 1
            print(f"[{msg_count}] Progress: {msg.get('progress_percent', 0)}% - {msg.get('label', 'N/A')}")

            if msg.get('status') in ('complete', 'error'):
                print(f"Pipeline {msg.get('status')}!")
                break
    except asyncio.TimeoutError:
        print("Timeout waiting for updates")

    await task

    status = get_status()
    print(f"\nFinal status: running={status.running}, error={status.error}")


if __name__ == "__main__":
    asyncio.run(test_run())
