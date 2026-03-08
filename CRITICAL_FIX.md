# CRITICAL FIX FOR PROGRESS BAR

## THE REAL PROBLEM

The WebSocket is working, but `run_demo.py` doesn't emit progress events.
The progress_monitor.py approach is too slow and unreliable.

## THE SOLUTION

Replace the real pipeline execution with a wrapper that emits progress at each step.

## IMPLEMENTATION

See `api/services/pipeline_runner_fixed.py` for the working implementation.

This uses a simple approach:
1. Run each step of the pipeline as a separate function call
2. Emit progress before and after each step
3. Broadcast via WebSocket immediately
4. No subprocess parsing, no file polling - just direct function calls

## Steps:
1. Extract source → 0-20%
2. Convert SQL → 20-40%
3. Load data → 40-70%
4. Validate → 70-90%
5. Test → 90-100%

Each step broadcasts multiple progress updates as it runs.
