"""Quick test script to verify backend and frontend are running"""
import requests
import sys

print("Testing MigData Services...")
print("=" * 50)

# Test Backend
print("\n1. Testing Backend (FastAPI on port 8000)...")
try:
    response = requests.get("http://localhost:8000/docs", timeout=5)
    if response.status_code == 200:
        print("   ✓ Backend is running on http://localhost:8000")
    else:
        print(f"   ✗ Backend returned status {response.status_code}")
except requests.exceptions.RequestException as e:
    print(f"   ✗ Backend is NOT running: {e}")
    print("   → Start with: python -m uvicorn api.main:app --reload")

# Test Frontend
print("\n2. Testing Frontend (React on port 5173)...")
try:
    response = requests.get("http://localhost:5173", timeout=5)
    if response.status_code == 200 and "MigData" in response.text:
        print("   ✓ Frontend is running on http://localhost:5173")
    else:
        print(f"   ✗ Frontend returned unexpected response")
except requests.exceptions.RequestException as e:
    print(f"   ✗ Frontend is NOT running: {e}")
    print("   → Start with: cd web && npm run dev")

# Test Backend API endpoint
print("\n3. Testing Backend API endpoint...")
try:
    response = requests.get("http://localhost:8000/api/config/", timeout=5)
    if response.status_code == 200:
        print("   ✓ Backend API is responding")
        data = response.json()
        print(f"   Source: {data.get('source_platform', 'unknown')}")
        print(f"   Target: {data.get('target_platform', 'unknown')}")
    else:
        print(f"   ✗ API returned status {response.status_code}")
except requests.exceptions.RequestException as e:
    print(f"   ✗ API is not responding: {e}")

print("\n" + "=" * 50)
print("\nIf all tests pass, open: http://localhost:5173")
print("Default login: admin / admin@123")
