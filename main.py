import os
import asyncio
import uvicorn
from fastapi import FastAPI, BackgroundTasks, WebSocket, WebSocketDisconnect
from dotenv import load_dotenv

load_dotenv(".env")

# Import execution engine logic
from execution_engine import compile_playbook, execute_playbook, client_registry

# Initialize FastAPI app
app = FastAPI(title="Rule Engine Orchestrator")

# Keep track of active background tasks per playbook so start/stop only affect
# the targeted live session instead of cancelling every playbook globally.
active_trading_tasks = {}
active_compile_tasks = {}


def _prune_finished_tasks(playbook_id: str) -> None:
    tasks = active_trading_tasks.get(playbook_id, [])
    # Filter out None and finished tasks
    remaining = [task for task in tasks if task is not None and not task.done()]
    if remaining:
        active_trading_tasks[playbook_id] = remaining
    else:
        active_trading_tasks.pop(playbook_id, None)


async def cancel_playbook_tasks(playbook_id: str) -> int:
    tasks = active_trading_tasks.pop(playbook_id, [])
    if not tasks:
        return 0

    print(f" [API] Cancelling {len(tasks)} active engine tasks for playbook {playbook_id}...")
    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    return len(tasks)


def _prune_finished_compile_task(playbook_id: str) -> None:
    task = active_compile_tasks.get(playbook_id)
    if task and task.done():
        active_compile_tasks.pop(playbook_id, None)


@app.get("/")
@app.get("/health")
async def handle_health():
    """Simple health check endpoint."""
    return {"status": "healthy", "service": "rule-engine-orchestrator"}


async def run_execute_in_background(playbook_id: str, session_id: str | None = None, user_id: str | None = None):
    """Wrapper to run the playbook execute flow and capture the background tasks."""
    tasks = await execute_playbook(playbook_id, client_registry, session_id=session_id, user_id=user_id)
    if tasks:
        active_trading_tasks[playbook_id] = [t for t in tasks if t is not None]
        for task in active_trading_tasks[playbook_id]:
            task.add_done_callback(lambda _task, pb=playbook_id: _prune_finished_tasks(pb))


async def run_compile_in_background(playbook_id: str) -> None:
    """Run playbook compilation without tying up the request lifecycle."""
    try:
        await compile_playbook(playbook_id)
    except Exception as exc:
        print(f" [API] Compile task failed for playbook {playbook_id}: {exc}")
    finally:
        active_compile_tasks.pop(playbook_id, None)


@app.post("/api/rules/compile")
async def compile_rule(playbook_id: str):
    """
    POST /api/rules/compile?playbook_id=abc
    Compiles the natural language prompt using the LLM and populates the database.
    """
    if not playbook_id:
        return {"error": "Missing 'playbook_id' in query parameters."}

    print(f" \n[API] Received Compile for Playbook: {playbook_id}")

    _prune_finished_compile_task(playbook_id)
    existing_task = active_compile_tasks.get(playbook_id)
    if existing_task and not existing_task.done():
        return {
            "status": "accepted",
            "message": "Engine compile already in progress for this playbook.",
            "playbook_id": playbook_id,
            "already_running": True,
        }

    task = asyncio.create_task(run_compile_in_background(playbook_id))
    active_compile_tasks[playbook_id] = task
    task.add_done_callback(lambda _task, pb=playbook_id: _prune_finished_compile_task(pb))

    return {
        "status": "accepted",
        "message": "Engine compile started. Compiling playbook via LLM.",
        "playbook_id": playbook_id,
        "already_running": False,
    }


@app.post("/api/rules/execute")
async def execute_rule(
    playbook_id: str,
    background_tasks: BackgroundTasks,
    session_id: str | None = None,
    user_id: str | None = None,
):
    """
    POST /api/rules/execute?playbook_id=abc
    Starts executing the previously compiled rules using the live websocket streams.
    """
    if not playbook_id:
        return {"error": "Missing 'playbook_id' in query parameters."}

    print(f" \n[API] Received Execute for Playbook: {playbook_id}")

    cancelled_tasks = await cancel_playbook_tasks(playbook_id)

    # Launch the execution flow in the background
    background_tasks.add_task(run_execute_in_background, playbook_id, session_id, user_id)

    return {
        "status": "success",
        "message": "Engine execute started. Running playbook against live streams.",
        "playbook_id": playbook_id,
        "session_id": session_id,
        "user_id": user_id,
        "cancelled_tasks": cancelled_tasks,
    }


@app.get("/api/rules/stop")
async def stop_playbook(playbook_id: str):
    """
    GET /api/rules/stop?playbook_id=abc
    Triggered by the frontend to stop active evaluating strategies.
    """
    if not playbook_id:
        return {"error": "Missing 'playbook_id' in query parameters."}

    print(f" \n[API] Received Stop for Playbook: {playbook_id}")

    cancelled_tasks = await cancel_playbook_tasks(playbook_id)

    return {
        "status": "success",
        "message": "Engine stopped. Active background tasks have been cancelled.",
        "playbook_id": playbook_id,
        "cancelled_tasks": cancelled_tasks,
    }


@app.websocket("/ws/engine-output")
async def websocket_handler(websocket: WebSocket):
    """
    WS /ws/engine-output
    Local Websocket endpoint so the frontend (or local GUI) can connect to this engine
    and watch the evaluation logs stream in real-time.
    """
    user_id = websocket.query_params.get("user_id")
    session_id = websocket.query_params.get("session_id")

    print(" [WEBSOCKET] Engine Result Viewer Connected")
    await client_registry.connect(websocket, user_id=user_id, session_id=session_id)
    
    try:
        while True:
            # We don't strictly expect messages from the viewer, but we must await
            # receive_text to keep the connection alive and catch disconnects natively.
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        print(" [WEBSOCKET] Engine Result Viewer Disconnected")
    except Exception as e:
        print(f" [WEBSOCKET] Connection closed with exception {e}")
    finally:
        await client_registry.disconnect(websocket, user_id=user_id, session_id=session_id)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    print(f" [SERVER] Starting FastAPI Orchestrator on port {port}...")
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
    except KeyboardInterrupt:
        print("\nEngine Orchestrator stopped.")
