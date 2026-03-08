"""
pipeline.py — Pipeline run/status/WebSocket endpoints
"""

import asyncio
import json

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect

from api.dependencies import CurrentUser
from api.models import PipelineRunRequest, PipelineStatusResponse
from api.services.pipeline_runner_v2 import (
    get_status,
    run_pipeline,
    subscribe,
    unsubscribe,
)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


@router.post("/run")
async def start_pipeline(body: PipelineRunRequest, _user: CurrentUser):
    job = get_status()
    if job.running:
        raise HTTPException(status_code=409, detail="Pipeline already running")

    asyncio.create_task(run_pipeline(body.source_platform, body.use_mock, body.selected_schemas))
    await asyncio.sleep(0.1)  # let it start

    updated = get_status()
    return {
        "message": "Pipeline started",
        "job_id": updated.job_id,
    }


@router.get("/status", response_model=PipelineStatusResponse)
def pipeline_status(_user: CurrentUser):
    job = get_status()
    return PipelineStatusResponse(
        running=job.running,
        current_step=job.current_step,
        total_steps=job.total_steps,
        label=job.label,
        job_id=job.job_id,
    )


@router.websocket("/ws/{job_id}")
async def pipeline_ws(websocket: WebSocket, job_id: str):
    await websocket.accept()
    q = subscribe()
    try:
        while True:
            msg = await asyncio.wait_for(q.get(), timeout=300)
            await websocket.send_text(json.dumps(msg))
            if msg.get("status") in ("complete", "error"):
                break
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    finally:
        unsubscribe(q)
        try:
            await websocket.close()
        except Exception:
            pass
