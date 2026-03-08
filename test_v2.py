import requests
import time

# Login
resp = requests.post('http://localhost:8000/api/auth/login', json={'username': 'admin', 'password': 'admin@123'})
if resp.status_code != 200:
    print('Login failed')
    exit(1)

token = resp.json()['token']
headers = {'Authorization': f'Bearer {token}'}

print('===== TESTING WITH V2 PIPELINE RUNNER =====\n')

# Start pipeline
resp = requests.post('http://localhost:8000/api/pipeline/run',
    json={'source_platform': 'snowflake', 'use_mock': True, 'selected_schemas': []},
    headers=headers)

print(f'Pipeline start: {resp.status_code}\n')
print('Progress updates:')

# Monitor
for i in range(60):
    time.sleep(0.2)
    status = requests.get('http://localhost:8000/api/pipeline/status', headers=headers).json()
    label = status['label'][:70]
    step = status['current_step']
    running = status['running']
    print(f'  {step}/5 | {label} (running={running})')
    if not running and i > 8:
        print('\nPipeline finished!')
        break
