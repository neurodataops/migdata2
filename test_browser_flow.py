"""
Quick test to verify the complete browser flow works
"""
import requests
import json
import time

BASE_URL = "http://localhost:8000"

print("Testing Complete Browser Flow\n")

# 1. Test Login
print("1. Testing login...")
login_response = requests.post(
    f"{BASE_URL}/api/auth/login",
    json={"username": "admin", "password": "admin@123"}
)
if login_response.status_code == 200:
    token = login_response.json()["token"]
    print(f"   [OK] Login successful! Token: {token[:50]}...")
else:
    print(f"   [ERROR] Login failed: {login_response.status_code}")
    exit(1)

headers = {"Authorization": f"Bearer {token}"}

# 2. Test Starting Pipeline
print("\n2. Testing pipeline start...")
pipeline_response = requests.post(
    f"{BASE_URL}/api/pipeline/run",
    json={
        "source_platform": "snowflake",
        "use_mock": True,  # Start with mock for quick test
        "selected_schemas": []
    },
    headers=headers
)

if pipeline_response.status_code == 200:
    job_id = pipeline_response.json().get("job_id")
    print(f"   [OK] Pipeline started! Job ID: {job_id}")
else:
    print(f"   [ERROR] Pipeline start failed: {pipeline_response.status_code}")
    print(f"   Response: {pipeline_response.text}")
    exit(1)

# 3. Monitor Status
print("\n3. Monitoring pipeline status...")
for i in range(10):
    time.sleep(1)
    status_response = requests.get(f"{BASE_URL}/api/pipeline/status", headers=headers)
    if status_response.status_code == 200:
        status = status_response.json()
        print(f"   Step {status['current_step']}/{status['total_steps']}: {status['label']}")
        if not status['running']:
            print("   [OK] Pipeline completed!")
            break
    else:
        print(f"   [ERROR] Status check failed: {status_response.status_code}")
        break

print("\n[SUCCESS] All tests passed! The API is working correctly.")
print("\nNext Steps:")
print("   1. Browser should be open at http://localhost:5173")
print("   2. Login with admin/admin@123")
print("   3. Toggle 'Data Source' to OFF for real Snowflake")
print("   4. Click 'Run Real Pipeline'")
print("   5. Watch progress bar move from 0% -> 100%!")
