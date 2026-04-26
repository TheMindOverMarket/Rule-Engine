import os
import json
import asyncio
from network.http_client import HTTPClient
from collections import defaultdict
from typing import Any, Dict, Optional, Set, Union
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from dotenv import load_dotenv

from broker.account_providers import AlpacaAccountProvider
from logic_adherence import (
    EngineState,
    build_logic_adherence_payload,
    build_session_history_context,
    flatten_market_payload,
    register_default_primitives,
)
from network.websocket_client import WebSocketClient
from network.market_data_hub import MarketDataHub
from engine import Playbook, RuleCategory, ContextBuilder
from llm_layer.schemas import ContextSkeletonSchema
from llm_layer.openai_client import OpenAILLMClient
from llm_layer.rule_parser import RuleParser

register_default_primitives()

# Load environment variables
load_dotenv(".env")

BACKEND_BASE_URL = os.getenv("TMOM_BACKEND_BASE_URL", "https://tmom-app-backend.onrender.com").rstrip("/")
BACKEND_WS_BASE_URL = os.getenv("TMOM_BACKEND_WS_BASE_URL", "").rstrip("/")
def _derive_ws_base_url(http_base_url: str) -> str:
    if http_base_url.startswith("https://"):
        return f"wss://{http_base_url[len('https://'):]}"
    if http_base_url.startswith("http://"):
        return f"ws://{http_base_url[len('http://'):]}"
    return http_base_url


if not BACKEND_WS_BASE_URL:
    BACKEND_WS_BASE_URL = _derive_ws_base_url(BACKEND_BASE_URL)


def build_backend_http_url(path: str) -> str:
    return f"{BACKEND_BASE_URL}{path}"


def build_backend_ws_url(path: str, **query_params: Optional[str]) -> str:
    base_url = f"{BACKEND_WS_BASE_URL}{path}"
    filtered_params = {key: value for key, value in query_params.items() if value}
    if not filtered_params:
        return base_url

    split_url = urlsplit(base_url)
    merged_params = dict(parse_qsl(split_url.query))
    merged_params.update(filtered_params)
    return urlunsplit(
        (
            split_url.scheme,
            split_url.netloc,
            split_url.path,
            urlencode(merged_params),
            split_url.fragment,
        )
    )


def normalize_market_symbol(value: Optional[str]) -> str:
    if not value:
        return ""

    normalized = value.strip().upper().replace("-", "/")
    if not normalized:
        return ""

    if "/" not in normalized:
        normalized = f"{normalized}/USD"

    base_asset, quote_asset = normalized.split("/", 1)
    base_asset = base_asset.strip()
    quote_asset = quote_asset.strip() or "USD"
    if not base_asset:
        return ""
    return f"{base_asset}/{quote_asset}"


def resolve_playbook_symbol(payload: Dict[str, Any]) -> str:
    context = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    return normalize_market_symbol(
        payload.get("symbol") or payload.get("market") or context.get("symbol")
    )


def ensure_context_symbol(context: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    synced_context = dict(context)
    synced_context["symbol"] = normalize_market_symbol(symbol)
    return synced_context


# Global persistence queue to prevent OOM from task accumulation
persistence_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
persistence_worker_task: Optional[asyncio.Task] = None

async def persistence_worker():
    """
    Background worker that processes the persistence queue sequentially.
    This prevents thousands of simultaneous HTTP requests to the backend.
    """
    print(" [PERSISTENCE] Worker started.")
    while True:
        try:
            session_id, payload, tick = await persistence_queue.get()
            await _perform_persistence(session_id, payload, tick)
            persistence_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f" [PERSISTENCE ERROR] Worker encountered error: {e}")
            await asyncio.sleep(1) # Cooldown on error

async def _perform_persistence(session_id: str, payload: Dict[str, Any], tick: Optional[int] = None):
    """Internal helper to actually send the data."""
    event_url = build_backend_http_url(f"/sessions/{session_id}/events")
    event_type = "DEVIATION" if payload.get("deviation") else "ADHERENCE"
    event_payload = {
        "type": event_type,
        "timestamp": payload.get("timestamp"),
        "tick": tick,
        "event_data": payload,
        "event_metadata": {
            "source": "rule_engine",
            "channel": "engine_output",
            "signal_type": event_type.lower(),
        },
    }

    try:
        async with await HTTPClient.post(
            event_url,
            json=event_payload,
            headers={"accept": "application/json"},
            timeout=10
        ) as response:
            if response.status not in (200, 201):
                err_text = await response.text()
                print(f"[ENGINE WARNING] Persistence failed (Status {response.status}): {err_text}")
    except Exception as exc:
        print(f"[ENGINE WARNING] Persistence exception for {session_id}: {exc}")

async def persist_backend_session_signal(
    session_id: str,
    payload: Dict[str, Any],
    tick: Optional[int] = None,
) -> None:
    """
    Enqueues a signal for persistence. Drops if the queue is full to protect memory.
    """
    global persistence_worker_task
    if persistence_worker_task is None or persistence_worker_task.done():
        persistence_worker_task = asyncio.create_task(persistence_worker())

    try:
        # Non-blocking put. If queue is full, we drop the signal to save memory.
        # It's better to lose one telemetry tick than to crash the service.
        persistence_queue.put_nowait((session_id, payload, tick))
    except asyncio.QueueFull:
        # If we are dropping signals, it means the backend is too slow or traffic is too high.
        # We only log this occasionally to avoid log-spamming.
        if tick and tick % 100 == 0:
            print(f" [ENGINE WARNING] Persistence queue full ({persistence_queue.qsize()}). Dropping tick {tick} for {session_id}.")


async def fetch_backend_session_replay(session_id: str) -> list[dict[str, Any]]:
    replay_url = build_backend_http_url(f"/sessions/{session_id}/replay")

    try:
        async with await HTTPClient.get(
            replay_url,
            headers={"accept": "application/json"},
        ) as response:
            if response.status == 200:
                payload = await response.json()
                if isinstance(payload, list):
                    return payload

            err_text = await response.text()
            print(
                f"[ENGINE WARNING] Failed to fetch session replay. "
                f"Status: {response.status}, Response: {err_text}"
            )
            return []
    except Exception as exc:
        print(f"[ENGINE WARNING] Could not fetch session replay: {exc}")
        return []


async def patch_backend_playbook(playbook_id: str, payload: Dict[str, Any]) -> bool:
    patch_url = build_backend_http_url(f"/playbooks/{playbook_id}")
    try:
        async with await HTTPClient.patch(
            patch_url,
            json=payload,
            headers={"accept": "application/json"},
        ) as patch_resp:
            if patch_resp.status in (200, 201, 204):
                return True

            err_text = await patch_resp.text()
            print(f"[ENGINE WARNING] Failed to patch playbook. Status: {patch_resp.status}, Response: {err_text}")
            return False
    except Exception as exc:
        print(f"[ENGINE WARNING] Could not patch playbook: {exc}")
        return False


class EngineOutputRegistry:
    def __init__(self) -> None:
        self._global_clients: Set[Any] = set()
        self._user_clients: Dict[str, Set[Any]] = defaultdict(set)
        self._session_clients: Dict[str, Set[Any]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def connect(self, websocket: Any, user_id: Optional[str] = None, session_id: Optional[str] = None) -> None:
        await websocket.accept()

        async with self._lock:
            if session_id:
                self._session_clients[session_id].add(websocket)
            elif user_id:
                self._user_clients[user_id].add(websocket)
            else:
                self._global_clients.add(websocket)

    async def disconnect(self, websocket: Any, user_id: Optional[str] = None, session_id: Optional[str] = None) -> None:
        async with self._lock:
            if session_id and session_id in self._session_clients:
                self._session_clients[session_id].discard(websocket)
                if not self._session_clients[session_id]:
                    del self._session_clients[session_id]
            elif user_id and user_id in self._user_clients:
                self._user_clients[user_id].discard(websocket)
                if not self._user_clients[user_id]:
                    del self._user_clients[user_id]
            else:
                self._global_clients.discard(websocket)

    async def broadcast(self, payload: Dict[str, Any], user_id: Optional[str] = None, session_id: Optional[str] = None) -> None:
        async with self._lock:
            # Aggregate all valid targets
            targets = set(self._global_clients)
            
            if session_id and session_id in self._session_clients:
                targets.update(self._session_clients[session_id])
            if user_id and user_id in self._user_clients:
                targets.update(self._user_clients[user_id])

            if not targets:
                # Optional: Log that we are broadcasting to empty void
                return

            for websocket in list(targets):
                try:
                    await websocket.send_json(payload)
                except Exception:
                    if session_id:
                        self._session_clients[session_id].discard(websocket)
                    elif user_id:
                        self._user_clients[user_id].discard(websocket)
                    else:
                        self._global_clients.discard(websocket)


client_registry = EngineOutputRegistry()

GLOBAL_ACCOUNT_FIELDS = ["equity", "buying_power", "cash", "daytrade_count", "open_positions"]


async def user_activity_handler(msg_or_dict: Union[str, Dict[str, Any]], state: EngineState):
    """
    Listens to the manual user-activity stream (e.g. click "Buy", "Sell", "Close").
    """
    try:
        if isinstance(msg_or_dict, str):
            data = json.loads(msg_or_dict)
        else:
            data = msg_or_dict
        
        if isinstance(data, dict) and data.get("message") == "unauthorized.":
             print(f" [USER STREAM ERROR] Received explicit 'unauthorized.' payload from backend stream")
             print(f"                     Full payload dumped: {data}")
             return
             
        print(f" [USER ACTION] Manual override detected: {data}")
        state.user_took_action = True
    except Exception as e:
        print(f" [USER STREAM ERROR] {e}")

async def run_market_engine(
    ws_url: str, 
    playbook: Playbook,
    context_builder: ContextBuilder,
    context_skeleton: ContextSkeletonSchema,
    output_registry: EngineOutputRegistry,
    state: EngineState,
    playbook_id: str,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
):
    target_symbol = normalize_market_symbol(context_skeleton.symbol)
    print("\n--- FRONTEND CONTEXT REQUEST SKELETON ---")
    if hasattr(context_skeleton, "model_dump_json"):
        print(context_skeleton.model_dump_json(indent=2))
    else:
        print(json.dumps(dict(context_skeleton), indent=2))
    print("-----------------------------------------\n")

    print(f"\n[ENGINE][MARKET] Initializing Market Engine for user_id: {user_id}, session_id: {session_id}")
    print(f" [MARKET] Connecting to {ws_url} via Hub...")
    evaluation_tick = 0
    
    # Cache session-level data once to avoid per-tick HTTP spam.
    session_history_cache = {
        "history": {"trades": [], "side_flip": [], "stop_loss": []},
        "event_history": [],
        "session_start_time": None,
        "last_refresh": 0
    }
    
    if session_id:
        print(f" [MARKET] Fetching initial session history for {session_id}...")
        initial_replay = await fetch_backend_session_replay(session_id)
        session_history_cache.update(build_session_history_context(initial_replay))
        session_history_cache["last_refresh"] = asyncio.get_event_loop().time()

    hub = await MarketDataHub.get_instance(ws_url)
    
    async def market_handler(msg_or_dict: Union[str, Dict[str, Any]]):
        nonlocal evaluation_tick
        try:
            if isinstance(msg_or_dict, str):
                data = json.loads(msg_or_dict)
            else:
                data = msg_or_dict
            
            # Explicitly catch backend unauthorized JSON payloads pushed over an open socket.
            if isinstance(data, dict) and data.get("message") == "unauthorized.":
                 print(f" [MARKET ENGINE ERROR] Received explicit 'unauthorized.' payload from backend stream ({ws_url})")
                 return

            # 1. Build Base Context from the market payload, including nested metrics.
            market_context = flatten_market_payload(data)
            incoming_symbol = normalize_market_symbol(market_context.get("symbol"))
            if incoming_symbol != target_symbol:
                return

            if session_id:
                # Periodic refresh of history (trades/stops) but not start_time
                now = asyncio.get_event_loop().time()
                if now - session_history_cache["last_refresh"] > 30.0:
                    # We only refresh trades/stops, start_time is immutable once found
                    latest_replay = await fetch_backend_session_replay(session_id)
                    latest_history = build_session_history_context(latest_replay)
                    session_history_cache["history"] = latest_history["history"]
                    session_history_cache["event_history"] = latest_history["event_history"]
                    if not session_history_cache["session_start_time"]:
                        session_history_cache["session_start_time"] = latest_history["session_start_time"]
                    session_history_cache["last_refresh"] = now

                market_context.update({
                    "history": session_history_cache["history"],
                    "event_history": session_history_cache["event_history"],
                    "session_start_time": session_history_cache["session_start_time"]
                })

                # 1.5 Temporal Integrity Check
                # Relative Session Timing Calculation
                start_time_str = session_history_cache["session_start_time"]
                curr_time_val = market_context.get("current_time") or market_context.get("timestamp")
                
                if start_time_str and curr_time_val:
                    try:
                        from datetime import datetime, timezone, timedelta
                        def to_dt(ts):
                            if isinstance(ts, (int, float)):
                                return datetime.fromtimestamp(ts, tz=timezone.utc)
                            # Handle Z suffix for fromisoformat
                            ts_clean = ts.replace("Z", "+00:00")
                            return datetime.fromisoformat(ts_clean)
                        
                        start_dt = to_dt(start_time_str)
                        curr_dt = to_dt(curr_time_val)
                        
                        # ROOT CAUSE FIX: Allow ticks from the last 24 hours to handle clock sync issues
                        # while still discarding genuinely ancient data.
                        if curr_dt < (start_dt - timedelta(hours=24)):
                            print(f" [ENGINE][MARKET] Discarding genuinely stale tick from {curr_dt} (Too old relative to session)")
                            return

                        delta = curr_dt - start_dt
                        market_context["minutes_since_start"] = delta.total_seconds() / 60.0
                        market_context["session_start_time_abs"] = start_time_str
                    except Exception as e:
                        print(f" [ENGINE WARNING] Failed temporal check/delta calculation: {e}")

            # 2. Hydrate Full Context (fetches account data if needed)
            full_context = context_builder.hydrate(
                base_context=market_context, 
                context_skeleton=context_skeleton
            )

            # 3. User Action & Payload
            user_action_bool = await state.get_and_reset_user_action()

            output_payload = build_logic_adherence_payload(
                playbook=playbook,
                context=full_context,
                user_action_bool=user_action_bool,
                state=state,
                playbook_id=playbook_id,
                session_id=session_id,
                user_id=user_id,
            )
            
            evaluation_tick += 1
            
            # Log Throttling: only log every 10 ticks, or on significant events (trigger/deviation)
            should_log = (evaluation_tick % 10 == 0) or output_payload['rule_triggered'] or output_payload['deviation']
            if should_log:
                price_display = output_payload["price"] if output_payload["price"] is not None else "N/A"
                print(
                    f"[{target_symbol}] TICK: {evaluation_tick:>5} | "
                    f"PRICE: {str(price_display):<8} | "
                    f"TRIG: {str(output_payload['rule_triggered']):<5} | "
                    f"ACT: {str(user_action_bool):<5} | "
                    f"DEV: {str(output_payload['deviation'])}"
                )
            
            await output_registry.broadcast(output_payload, user_id=user_id, session_id=session_id)

            if session_id:
                # Fire and forget persistence via size-limited queue
                await persist_backend_session_signal(
                    session_id=session_id,
                    payload=output_payload,
                    tick=evaluation_tick,
                )
                        
        except Exception as e:
            import traceback
            print(f" [MARKET ENGINE CRITICAL ERROR] Failed during market_handler: {e}")
            traceback.print_exc()

    # Subscribe to the hub instead of listening directly to a local client
    subscription_id = await hub.subscribe(market_handler)
    print(f" [MARKET] Subscribed to Hub with ID: {subscription_id}")
    
    # We return a function that can be used to unsubscribe later
    async def cleanup():
        await hub.unsubscribe(market_handler)
        
    return cleanup


async def compile_playbook(playbook_id: str):
    """
    Compilation flow:
    1. Fetch rule from Supabase
    2. Parse it with LLM
    3. Persist tables (Rules, Conditions, Edges)
    4. Patch derived context and compiled JSON back to Supabase
    """
    print(f"\n[ENGINE] Starting compilation for Playbook: {playbook_id}")

    # 1. Fetch raw user prompt from Supabase
    fetch_url = build_backend_http_url(f"/playbooks/{playbook_id}")
    prompt_text = ""
    persisted_symbol = ""
    playbook_data: Dict[str, Any] = {}
    
    try:
        async with await HTTPClient.get(fetch_url, headers={"accept": "application/json"}) as resp:
            if resp.status == 200:
                playbook_data = await resp.json()
                persisted_symbol = resolve_playbook_symbol(playbook_data)
                prompt_text = playbook_data.get("original_nl_input") or playbook_data.get("rule_text", "")
                print(f"[ENGINE] Successfully fetched prompt ({len(prompt_text)} chars).")
            else:
                print(f"[ENGINE ERROR] Failed to fetch playbook from Supabase. Status: {resp.status}")
                return
    except Exception as e:
        print(f"[ENGINE ERROR] Could not reach Supabase to fetch playbook: {e}")
        return
            
    if not prompt_text:
        print("[ENGINE ERROR] Prompt text is empty. Cannot compile engine.")
        await patch_backend_playbook(playbook_id, {"generation_status": "FAILED"})
        return
    if not persisted_symbol:
        reason = "Playbook is missing a persisted market symbol."
        print(f"[ENGINE ERROR] {reason}")
        await patch_backend_playbook(playbook_id, {"generation_status": "FAILED", "failure_reason": reason})
        return

    # 2. Parse the rule using the LLM
    llm_client = OpenAILLMClient(model="gpt-4-turbo")
    parser = RuleParser(llm_client, category=RuleCategory.ENTRY)
    
    print(f"[ENGINE] Parsing rule playbook via Chat...")
    try:
        chat_history = playbook_data.get("chat_history") or [{"role": "user", "content": prompt_text}]
        playbook, context_skeleton, clarification_reason = await asyncio.to_thread(parser.parse_chat, chat_history)
        
        clarification_reason = clarification_reason or ""

        if playbook is None:
            print(f"[ENGINE] Response status check: {clarification_reason}")
            
            # Case A: Pure Greeting/Noise (Flagged with GREETING: prefix in parser)
            if clarification_reason.startswith("GREETING:"):
                clean_msg = clarification_reason.replace("GREETING:", "")
                chat_history.append({"role": "assistant", "content": clean_msg})
                await patch_backend_playbook(playbook_id, {
                    "generation_status": "INITIALIZING",
                    "chat_history": chat_history
                })
                return

            # Case B: Standard Strategy Clarification
            print(f"[ENGINE] Needs clarification: {clarification_reason}")
            chat_history.append({"role": "assistant", "content": clarification_reason})
            await patch_backend_playbook(playbook_id, {
                "generation_status": "INCOMPLETE",
                "chat_history": chat_history
            })
            return

        if context_skeleton is None:
            reason = "Parser returned no context skeleton."
            print(f"[ENGINE ERROR] {reason}")
            await patch_backend_playbook(playbook_id, {"generation_status": "FAILED", "failure_reason": reason})
            return
        skeleton_dict = dict(context_skeleton) if not hasattr(context_skeleton, "model_dump") else context_skeleton.model_dump()
        skeleton_dict = ensure_context_symbol(skeleton_dict, persisted_symbol)
        
        # 3. Persist the parsed rules/conditions to backend DB
        from populate_tables import populate_playbook_tables
        try:
            await populate_playbook_tables(playbook_id, playbook)
        except Exception as pop_err:
            reason = f"DB Population failed: {pop_err}"
            print(f"[ENGINE WARNING] {reason}")
            await patch_backend_playbook(playbook_id, {"generation_status": "FAILED", "failure_reason": reason})
            return

    except Exception as e:
        print(f"[ENGINE ERROR] Failed to parse playbook: {e}")
        await patch_backend_playbook(playbook_id, {"generation_status": "FAILED", "failure_reason": str(e)})
        return

    # 4. Patch the Context Skeleton & Compiled Rules back to Supabase
    compiled_rules = []
    for rule in playbook.rules:
        compiled_rules.append({
            "name": rule.name,
            "id": str(rule.id) if rule.id else None,
            "category": rule.category.name,
            "extensions": [
                {"id": ext.id, "primitive": ext.primitive_name, "params": ext.params}
                for ext in rule.extensions.values()
            ],
            "conditions": rule.conditions
        })
        
    patch_data = {
        "symbol": persisted_symbol,
        "market": persisted_symbol,
        "context": skeleton_dict,
    }
    patch_data["context"]["compiled_rules"] = compiled_rules

    patch_data["generation_status"] = "COMPLETED"
    if await patch_backend_playbook(playbook_id, patch_data):
        print("[ENGINE] Successfully patched Context Skeleton and Compiled Rules to database.")


async def execute_playbook(
    playbook_id: str,
    output_registry: EngineOutputRegistry,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
):
    """
    Execution flow:
    1. Fetch compiled Context Skeleton & Rules from Supabase
    2. Reconstruct Playbook object in memory
    3. Spin up the trading engine loops
    """
    print(f"\n[ENGINE][EXECUTE] Starting execution context")
    print(f"         Playbook: {playbook_id}")
    print(f"         Session:  {session_id}")
    print(f"         User:     {user_id}")

    fetch_url = build_backend_http_url(f"/playbooks/{playbook_id}")
    resolved_symbol = ""
    try:
        async with await HTTPClient.get(fetch_url, headers={"accept": "application/json"}) as resp:
            if resp.status == 200:
                data = await resp.json()
                resolved_symbol = resolve_playbook_symbol(data)
                # Priority: 1. API Parameter, 2. Database Record
                effective_user_id = user_id or data.get("user_id")
                print(f" [ENGINE] Resolved execute user_id: {effective_user_id} (API: {user_id}, DB: {data.get('user_id')})")
                print(f" [ENGINE] Resolved execute symbol: {resolved_symbol}")
                user_id = effective_user_id
            else:
                print(f"[ENGINE ERROR] Failed to fetch playbook to execute. Status: {resp.status}")
                return None
    except Exception as e:
        print(f"[ENGINE ERROR] Could not reach Supabase for execute: {e}")
        return None

    if not resolved_symbol:
        print("[ENGINE ERROR] Playbook missing market symbol. Execution cannot start.")
        return None

    context_data = data.get("context", {})
    if not context_data:
        print("[ENGINE ERROR] Playbook missing compiled context. Please compile first.")
        return None

    compiled_rules = context_data.get("compiled_rules", [])
    
    # 2. Reconstruct Playbook
    from engine import Playbook, RuleBlock
    playbook = Playbook()
    for rule_data in compiled_rules:
        cat_enum = RuleCategory[rule_data["category"]]
        rule_block = RuleBlock(category=cat_enum, skeleton=rule_data)
        rule_block.id = rule_data.get("id")
        playbook.add_rule(rule_block)

    # Need to remove our injected 'compiled_rules' before validating ContextSkeletonSchema
    safe_context_data = {k: v for k, v in context_data.items() if k != "compiled_rules"}
    safe_context_data = ensure_context_symbol(safe_context_data, resolved_symbol)
    context_skeleton = ContextSkeletonSchema(**safe_context_data)

    # 3. Spin up trading engine loops
    alpaca_provider = AlpacaAccountProvider(paper=True)
    
    context_builder = ContextBuilder(
        account_provider=alpaca_provider,
        user_action_provider=None,
        global_account_fields=GLOBAL_ACCOUNT_FIELDS
    )

    user_ws_url = build_backend_ws_url("/ws/user-activity", user_id=user_id, session_id=session_id)
    market_ws_url = build_backend_ws_url("/ws/market-state", user_id=user_id)
    
    user_hub = await MarketDataHub.get_instance(user_ws_url)
    state = EngineState()

    async def handle_user_activity(msg: str):
        await user_activity_handler(msg, state)

    print(f"[ENGINE] Subscribing via Hub to:")
    print(f"         - User Activity: {user_ws_url}")
    print(f"         - Market State:  {market_ws_url}")
    user_sub_id = await user_hub.subscribe(handle_user_activity)
    
    # Wrap run_market_engine in a task
    market_engine_cleanup_fn = None
    
    async def market_wrapper():
        nonlocal market_engine_cleanup_fn
        market_engine_cleanup_fn = await run_market_engine(
            market_ws_url, 
            playbook, 
            context_builder, 
            context_skeleton,
            output_registry,
            state,
            playbook_id,
            session_id=session_id,
            user_id=user_id,
        )

    task_market = asyncio.create_task(market_wrapper())

    # Create a wrapper task that manages both
    async def master_task():
        try:
            await task_market
            print(f"[ENGINE] Master task for playbook {playbook_id} is now running. Waiting for events...")
            import sys
            sys.stdout.flush()
            
            # Wait indefinitely until cancelled by the API or session shutdown
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            print(f"[ENGINE] Master task for playbook {playbook_id} received cancellation.")
        finally:
            print(f"[ENGINE] Master task for playbook {playbook_id} finishing, cleaning up subscriptions...")
            await user_hub.unsubscribe(handle_user_activity)
            if market_engine_cleanup_fn:
                await market_engine_cleanup_fn()
            import sys
            sys.stdout.flush()

    final_task = asyncio.create_task(master_task(), name=f"master_task_{playbook_id}")
    return [final_task]

async def stream_compile_playbook(playbook_id: Optional[str] = None, chat_history: Optional[list] = None):
    """
    Streaming version of compile_playbook:
    1. Fetch raw user prompt (if playbook_id provided)
    2. Stream tokens from LLM
    3. NO automatic persistence (to support manual approval workflow)
    """
    if playbook_id:
        fetch_url = build_backend_http_url(f"/playbooks/{playbook_id}")
        playbook_data = {}
        async with await HTTPClient.get(fetch_url) as resp:
            if resp.status == 200:
                playbook_data = await resp.json()
            else:
                yield f"Error: Failed to fetch playbook {playbook_id}"
                return
        prompt_text = playbook_data.get("original_nl_input") or playbook_data.get("rule_text", "")
        chat_history = playbook_data.get("chat_history") or [{"role": "user", "content": prompt_text}]
    
    if not chat_history:
        yield "Error: No chat history provided for streaming."
        return

    llm_client = OpenAILLMClient(model="gpt-4o-mini")
    parser = RuleParser(llm_client, category=RuleCategory.ENTRY)

    async for token in parser.stream_parse_chat(chat_history):
        yield token

async def preview_compile_playbook(chat_history: list) -> Dict[str, Any]:
    """
    Non-streaming preview:
    1. Parse the chat history with LLM
    2. Return the structured response without persisting to DB.
    """
    print(f"[ENGINE] Generating PREVIEW for chat history ({len(chat_history)} turns).")
    llm_client = OpenAILLMClient(model="gpt-4o-mini")
    parser = RuleParser(llm_client, category=RuleCategory.ENTRY)
    
    try:
        # returns (playbook, context_skeleton, clarification_reason)
        # But we actually want the raw LLMResponseSchema content for the frontend
        # Let's adjust parser to expose the raw response if possible or reconstruct here.
        # For now, we'll use a direct LLM call if needed, but parser.parse_chat is better.
        playbook, context_skeleton, clarification_reason = await asyncio.to_thread(parser.parse_chat, chat_history)
        
        if playbook is None:
            status = "greeting" if (clarification_reason and clarification_reason.startswith("GREETING:")) else "needs_clarification"
            dialogue = clarification_reason.replace("GREETING:", "") if clarification_reason else "I need more details to clarify your strategy."
            return {
                "status": status,
                "dialogue": dialogue,
                "rules": [],
                "context_skeleton": None
            }

        compiled_rules = []
        for rule in playbook.rules:
            compiled_rules.append({
                "name": rule.name,
                "category": rule.category.name,
                "extensions": [
                    {"id": ext.id, "primitive": ext.primitive_name, "params": ext.params}
                    for ext in rule.extensions.values()
                ],
                "conditions": rule.conditions
            })

        return {
            "status": "ok",
            "dialogue": clarification_reason or "I've structured your strategy logic below. Please review and confirm to deploy.",
            "rules": compiled_rules,
            "context_skeleton": context_skeleton.model_dump() if context_skeleton else None
        }
    except Exception as e:
        print(f"[ENGINE ERROR] Preview failed: {e}")
        return {
            "status": "unsupported",
            "dialogue": f"I encountered an error while parsing: {str(e)}",
            "rules": [],
            "context_skeleton": None
        }
