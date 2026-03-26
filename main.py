import os
import asyncio
import uvicorn
from fastapi import FastAPI, BackgroundTasks, WebSocket, WebSocketDisconnect
from dotenv import load_dotenv

load_dotenv(".env")

# Import execution engine logic
from execution_engine import process_new_playbook, connected_clients

# Initialize FastAPI app
app = FastAPI(title="Rule Engine Orchestrator")

# Keep track of active background WebSocket tasks so we can cancel them
# if the frontend triggers a new rule.
active_trading_tasks = []


@app.get("/")
@app.get("/health")
async def handle_health():
    """Simple health check endpoint."""
    return {"status": "healthy", "service": "rule-engine-orchestrator"}


async def run_in_background(user_id: str, playbook_id: str):
    """Wrapper to run the playbook flow and capture the background tasks."""
    global active_trading_tasks
    tasks = await process_new_playbook(user_id, playbook_id, connected_clients)
    if tasks:
        active_trading_tasks.extend(tasks)


@app.get("/api/rules/trigger")
async def trigger_playbook(user_id: str, playbook_id: str, background_tasks: BackgroundTasks):
    """
    GET /api/rules/trigger?user_id=123&playbook_id=abc
    Triggered by the frontend when a user saves a new rule to the database.
    """
    if not user_id or not playbook_id:
        return {"error": "Missing 'user_id' or 'playbook_id' in query parameters."}

    print(f" \n[API] Received Trigger for User: {user_id} | Playbook: {playbook_id}")

    # Cancel any existing market engine tasks so we don't have conflicting rules trading
    global active_trading_tasks
    if active_trading_tasks:
        print(f" [API] Cancelling {len(active_trading_tasks)} previously active engine tasks...")
        for task in active_trading_tasks:
            task.cancel()
        active_trading_tasks.clear()

    # Launch the new playbook execution flow in the background
    background_tasks.add_task(run_in_background, user_id, playbook_id)

    # Immediately respond to frontend so they aren't blocked waiting for LLM parsing
    return {
        "status": "success",
        "message": "Engine triggered. Fetching and applying playbook in the background."
    }


@app.get("/api/rules/stop")
async def stop_playbook(user_id: str, playbook_id: str):
    """
    GET /api/rules/stop?user_id=123&playbook_id=abc
    Triggered by the frontend to stop active evaluating strategies.
    """
    if not user_id or not playbook_id:
        return {"error": "Missing 'user_id' or 'playbook_id' in query parameters."}

    print(f" \n[API] Received Stop for User: {user_id} | Playbook: {playbook_id}")

    global active_trading_tasks
    if active_trading_tasks:
        print(f" [API] Cancelling {len(active_trading_tasks)} active engine tasks...")
        for task in active_trading_tasks:
            task.cancel()
        active_trading_tasks.clear()

    return {
        "status": "success",
        "message": "Engine stopped. Active background tasks have been cancelled."
    }


@app.websocket("/ws/engine-output")
async def websocket_handler(websocket: WebSocket):
    """
    WS /ws/engine-output
    Local Websocket endpoint so the frontend (or local GUI) can connect to this engine
    and watch the evaluation logs stream in real-time.
    """
    await websocket.accept()
    print(" [WEBSOCKET] Engine Result Viewer Connected")
    connected_clients.add(websocket)
    
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
        if websocket in connected_clients:
            connected_clients.remove(websocket)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    print(f" [SERVER] Starting FastAPI Orchestrator on port {port}...")
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
    except KeyboardInterrupt:
        print("\nEngine Orchestrator stopped.")
