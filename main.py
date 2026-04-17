import os
import asyncio
import uuid
import sys
import aiohttp
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, BackgroundTasks, WebSocket, WebSocketDisconnect, Query, Body
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv

# Unique identifier for this instance to help diagnose Render deployment logs
INSTANCE_ID = str(uuid.uuid4())[:8]

load_dotenv(".env")

# Import execution engine logic
from execution_engine import compile_playbook, stream_compile_playbook, execute_playbook, client_registry, BACKEND_BASE_URL

# Initialize FastAPI app
app = FastAPI(title="Rule Engine Orchestrator")

@app.on_event("startup")
async def startup_event():
    print(f" [LIFECYCLE] Rule Engine Instance {INSTANCE_ID} starting...")
    sys.stdout.flush()
    # Trigger recovery of any sessions that were active before the restart
    asyncio.create_task(auto_recover_active_sessions())
    # Start internal heartbeat
    asyncio.create_task(heartbeat_loop())

@app.on_event("shutdown")
async def shutdown_event():
    print(f" [LIFECYCLE] Rule Engine Instance {INSTANCE_ID} shutting down...")
    sys.stdout.flush()

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


async def heartbeat_loop():
    """Periodic pulse to keep the instance warm and event loop verified."""
    print(f" [LIFECYCLE] Heartbeat loop started for instance {INSTANCE_ID}.")
    while True:
        try:
            await asyncio.sleep(300) # Every 5 minutes
            print(f" [HEARTBEAT] Instance {INSTANCE_ID} is alive. OS: {sys.platform}")
            sys.stdout.flush()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f" [HEARTBEAT ERROR] {e}")


async def auto_recover_active_sessions(attempts: int = 8, delay_secs: int = 15):
    """
    On startup, query the backend for all sessions marked as STARTED
    and automatically re-initialize their execution engine tasks.
    Retries on failure to handle transient backbone API unavailability.
    """
    print(f" [LIFECYCLE] Initiating auto-recovery for active sessions (Attempt {9 - attempts}/8)...")
    sys.stdout.flush()
    
    # Wait a few seconds for networking to fully stabilize inside the container
    await asyncio.sleep(2 if attempts == 8 else delay_secs)
    
    sessions_url = f"{BACKEND_BASE_URL}/sessions/"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(sessions_url, headers={"accept": "application/json"}) as resp:
                if resp.status != 200:
                    err_body = await resp.text()
                    print(f" [LIFECYCLE ERROR] Failed to fetch sessions. Status: {resp.status}")
                    print(f" [LIFECYCLE DIAGNOSTIC] Response Body: {err_body[:500]}") # Only first 500 chars
                    
                    if attempts > 1 and resp.status >= 500:
                        print(f" [LIFECYCLE] Transient backend error detected. Retrying in {delay_secs}s...")
                        sys.stdout.flush()
                        return await auto_recover_active_sessions(attempts - 1, delay_secs)
                    return
                
                all_sessions = await resp.json()
                active_sessions = [s for s in all_sessions if s.get("status") == "STARTED"]
                
                if not active_sessions:
                    print(f" [LIFECYCLE] No active sessions found to recover.")
                    return
                
                print(f" [LIFECYCLE] Found {len(active_sessions)} active sessions. Resuming...")
                for sess in active_sessions:
                    playbook_id = sess.get("playbook_id")
                    session_id = sess.get("id")
                    user_id = sess.get("user_id")
                    
                    if not playbook_id:
                        continue
                        
                    print(f" [LIFECYCLE] Recovering Playbook {playbook_id} for Session {session_id}...")
                    await run_execute_in_background(playbook_id, session_id=session_id, user_id=user_id)
                    
                print(f" [LIFECYCLE] Recovery completed.")
    except Exception as e:
        print(f" [LIFECYCLE ERROR] Recovery failed with exception: {e}")
        if attempts > 1:
            print(f" [LIFECYCLE] Connection issue. Retrying in {delay_secs}s...")
            sys.stdout.flush()
            return await auto_recover_active_sessions(attempts - 1, delay_secs)
    finally:
        sys.stdout.flush()



@app.get("/")
@app.get("/health")
async def handle_health():
    """Simple health check endpoint."""
    print(f" [HEALTH] Instance {INSTANCE_ID} received poke from heartbeat/probe.")
    sys.stdout.flush()
    return {
        "status": "healthy", 
        "service": "rule-engine-orchestrator",
        "instance_id": INSTANCE_ID
    }


async def run_execute_in_background(playbook_id: str, session_id: str | None = None, user_id: str | None = None):
    """Wrapper to run the playbook execute flow and capture the background tasks."""
    print(f" [API] Launching EXECUTE background task for session {session_id} and user {user_id}")
    sys.stdout.flush()
    
    try:
        tasks = await execute_playbook(playbook_id, client_registry, session_id=session_id, user_id=user_id)
        if tasks:
            active_trading_tasks[playbook_id] = [t for t in tasks if t is not None]
            for task in active_trading_tasks[playbook_id]:
                task.add_done_callback(lambda _task, pb=playbook_id: _prune_finished_tasks(pb))
            print(f" [API] Successfully started {len(active_trading_tasks[playbook_id])} live engine tasks.")
        else:
            print(f" [API] Failed to start live engine tasks for playbook {playbook_id}.")
    except Exception as exc:
        print(f" [API CRITICAL] Error launching EXECUTE background task: {exc}")
        import traceback
        traceback.print_exc()
    finally:
        sys.stdout.flush()


async def run_compile_in_background(playbook_id: str) -> None:
    """Run playbook compilation without tying up the request lifecycle."""
    try:
        await compile_playbook(playbook_id)
    except Exception as exc:
        print(f" [API] Compile task failed for playbook {playbook_id}: {exc}")
    finally:
        active_compile_tasks.pop(playbook_id, None)


@app.post("/api/rules/preview")
async def preview_rule(turn: dict = Body(...)):
    """
    POST /api/rules/preview
    Stateless preview: Takes chat_history and returns structured logic.
    """
    chat_history = turn.get("chat_history")
    if not chat_history:
        return {"error": "Missing 'chat_history' in request body."}

    print(f" \n[API] Received Preview Request for Chat History")
    from execution_engine import preview_compile_playbook
    result = await preview_compile_playbook(chat_history)
    return result

@app.post("/api/rules/explain_deviation")
async def explain_deviation(payload: dict = Body(...)):
    """
    POST /api/rules/explain_deviation
    Takes Playbook Text and Event Data, returns an LLM-generated explanation.
    """
    playbook_text = payload.get("playbook_text")
    event_data = payload.get("event_data")
    if not playbook_text or not event_data:
        return {"error": "Missing playbook_text or event_data in payload"}

    from llm_layer.openai_client import OpenAILLMClient
    from llm_layer.reasoner import DeviationReasoner
    import asyncio

    llm_client = OpenAILLMClient(model="gpt-4o-mini")
    reasoner = DeviationReasoner(llm_client)

    # Offload LLM call to async thread to unblock fastApi loop
    reasoning = await asyncio.to_thread(reasoner.explain_deviation, playbook_text, event_data)
    
    return {"status": "success", "reasoning": reasoning}


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


@app.get("/api/rules/stream")
@app.post("/api/rules/stream")
async def stream_rule(playbook_id: Optional[str] = Query(None), turn: Optional[dict] = Body(None)):
    """
    GET/POST /api/rules/stream
    Streams the LLM response for conversation/clarification.
    Supports both stateful (playbook_id) and stateless (chat_history via POST) flows.
    """
    chat_history = None
    if turn:
        chat_history = turn.get("chat_history")

    print(f" \n[API] Received Stream Request (playbook_id: {playbook_id}, has_history: {bool(chat_history)})")

    return StreamingResponse(
        stream_compile_playbook(playbook_id=playbook_id, chat_history=chat_history),
        media_type="text/event-stream"
    )


@app.post("/api/rules/execute")
async def execute_rule(
    playbook_id: str,
    background_tasks: BackgroundTasks,
    session_id: str | None = Query(None),
    user_id: str | None = Query(None),
):
    """
    Starts executing the previously compiled rules using the live websocket streams.
    """
    if not playbook_id:
        return {"status": "error", "message": "Missing 'playbook_id' in query parameters."}

    print(f"\n [API] Received Execute request for playbook {playbook_id}")
    print(f"       Session:  {session_id}")
    print(f"       User:     {user_id}")

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

    print(f" [WEBSOCKET] Engine Result Viewer Connected (user:{user_id}, session:{session_id})")
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
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    print(f" [SERVER] Instance {INSTANCE_ID} starting FastAPI Orchestrator on port {port}...")
    sys.stdout.flush()
    try:
        uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
    except KeyboardInterrupt:
        print(f"\n [SERVER] Instance {INSTANCE_ID} stopped via KeyboardInterrupt.")
