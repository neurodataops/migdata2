"""
Test WebSocket progress updates
"""
import asyncio
import websockets
import json
import requests

BASE_URL = "http://localhost:8000"

async def test_websocket():
    # 1. Login first
    login_resp = requests.post(
        f"{BASE_URL}/api/auth/login",
        json={"username": "admin", "password": "admin@123"}
    )
    token = login_resp.json()["token"]
    headers = {"Authorization": f"Bearer {token}"}

    # 2. Start pipeline
    pipeline_resp = requests.post(
        f"{BASE_URL}/api/pipeline/run",
        json={
            "source_platform": "snowflake",
            "use_mock": True,
            "selected_schemas": []
        },
        headers=headers
    )
    job_id = pipeline_resp.json().get("job_id")
    print(f"Pipeline started: Job ID = {job_id}\n")

    # 3. Connect to WebSocket
    ws_url = f"ws://localhost:8000/api/pipeline/ws/{job_id}"

    print("Connecting to WebSocket...")
    async with websockets.connect(ws_url) as websocket:
        print("Connected! Listening for progress updates...\n")

        msg_count = 0
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=30)
                data = json.loads(message)
                msg_count += 1

                progress = data.get('progress_percent', 0)
                label = data.get('label', 'N/A')
                status = data.get('status', 'unknown')
                step = data.get('step', 0)

                print(f"[{msg_count}] {progress}% - Step {step} - {label} ({status})")

                if status in ('complete', 'error'):
                    print(f"\nPipeline {status}!")
                    break

            except asyncio.TimeoutError:
                print("Timeout waiting for message")
                break
            except Exception as e:
                print(f"Error: {e}")
                break

if __name__ == "__main__":
    asyncio.run(test_websocket())
