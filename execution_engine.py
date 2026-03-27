import os
import json
import asyncio
import aiohttp
from collections import defaultdict
from typing import Any, Dict, Optional, Set
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from dotenv import load_dotenv

from broker.account_providers import AlpacaAccountProvider
from network.websocket_client import WebSocketClient
from engine import Playbook, RuleCategory, ContextBuilder, Primitive, PrimitiveRegistry
from primitives import (
    comparison_evaluator,
    temporal_gate_evaluator,
    account_comparison_evaluator,
    set_membership_evaluator,
    rate_limit_evaluator,
    accumulation_evaluator,
    sequence_evaluator,
)
from llm_layer.schemas import ContextSkeletonSchema
from llm_layer.openai_client import OpenAILLMClient
from llm_layer.rule_parser import RuleParser

# Register Primitives
def register_primitives():
    print("registering primitives")
    if "comparison" not in PrimitiveRegistry._registry:
        PrimitiveRegistry.register(Primitive("comparison", comparison_evaluator, required_context=["price"]))
    if "temporal_gate" not in PrimitiveRegistry._registry:
        PrimitiveRegistry.register(Primitive("temporal_gate", temporal_gate_evaluator, required_context=["current_time"]))
    if "account_comparison" not in PrimitiveRegistry._registry:
        PrimitiveRegistry.register(Primitive("account_comparison", account_comparison_evaluator))
    if "set_membership" not in PrimitiveRegistry._registry:
        PrimitiveRegistry.register(Primitive("set_membership", set_membership_evaluator))
    if "rate_limit" not in PrimitiveRegistry._registry:
        PrimitiveRegistry.register(Primitive("rate_limit", rate_limit_evaluator))
    if "accumulation" not in PrimitiveRegistry._registry:
        PrimitiveRegistry.register(Primitive("accumulation", accumulation_evaluator))
    if "sequence" not in PrimitiveRegistry._registry:
        PrimitiveRegistry.register(Primitive("sequence", sequence_evaluator))

register_primitives()

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


async def persist_backend_session_signal(
    session_id: str,
    payload: Dict[str, Any],
    tick: Optional[int] = None,
) -> bool:
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

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                event_url,
                json=event_payload,
                headers={"accept": "application/json"},
            ) as response:
                if response.status in (200, 201):
                    return True

                err_text = await response.text()
                print(
                    f"[ENGINE WARNING] Failed to persist session signal. "
                    f"Status: {response.status}, Response: {err_text}"
                )
                return False
        except Exception as exc:
            print(f"[ENGINE WARNING] Could not persist session signal: {exc}")
            return False


async def patch_backend_playbook(playbook_id: str, payload: Dict[str, Any]) -> bool:
    patch_url = build_backend_http_url(f"/playbooks/{playbook_id}")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.patch(
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
            if session_id:
                targets = list(self._session_clients.get(session_id, []))
            elif user_id:
                targets = list(self._user_clients.get(user_id, []))
            else:
                targets = list(self._global_clients)

            for websocket in targets:
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

# Mock State Manager
class EngineState:
    def __init__(self):
        self.user_took_action = False
    
    async def get_and_reset_user_action(self) -> bool:
        val = self.user_took_action
        self.user_took_action = False
        return val

GLOBAL_ACCOUNT_FIELDS = ["equity", "buying_power", "cash", "daytrade_count", "open_positions"]

def _flatten_market_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}

    for key, value in data.items():
        if key in {"metrics", "indicator_values"}:
            continue
        flattened[key] = value

    metrics = data.get("metrics", {})
    if isinstance(metrics, dict):
        for key, value in metrics.items():
            flattened.setdefault(key, value)

    indicator_values = data.get("indicator_values", {})
    if isinstance(indicator_values, dict):
        for timeframe, timeframe_metrics in indicator_values.items():
            if not isinstance(timeframe_metrics, dict):
                continue
            for metric_name, metric_value in timeframe_metrics.items():
                flattened_key = metric_name if timeframe == "1m" else f"{metric_name}_{timeframe}"
                flattened.setdefault(flattened_key, metric_value)

    if "current_time" not in flattened and "timestamp" in flattened:
        flattened["current_time"] = flattened["timestamp"]

    return flattened


async def user_activity_handler(msg: str, state: EngineState):
    """
    Listens to the manual user-activity stream (e.g. click "Buy", "Sell", "Close").
    """
    try:
        data = json.loads(msg)
        
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
    print("\n--- FRONTEND CONTEXT REQUEST SKELETON ---")
    if hasattr(context_skeleton, "model_dump_json"):
        print(context_skeleton.model_dump_json(indent=2))
    else:
        print(json.dumps(dict(context_skeleton), indent=2))
    print("-----------------------------------------\n")

    client = WebSocketClient(ws_url)
    print(f" [MARKET] Connecting to {ws_url}...")
    evaluation_tick = 0
    
    async def market_handler(msg: str):
        nonlocal evaluation_tick
        try:
            print(" [MARKET] -> Loading JSON message")
            data = json.loads(msg)
            
            # Explicitly catch backend unauthorized JSON payloads pushed over an open socket.
            if isinstance(data, dict) and data.get("message") == "unauthorized.":
                 print(f" [MARKET ENGINE ERROR] Received explicit 'unauthorized.' payload from backend stream ({ws_url})")
                 print(f"                       Full payload dumped: {data}")
                 return

            print(" [MARKET] -> Extracting Base Context")
            
            # 1. Build Base Context from the market payload, including nested metrics.
            market_context = _flatten_market_payload(data)

            print(" [MARKET] -> Hydrating Full Context")
            
            # 2. Hydrate Full Context (fetches account data if needed)
            full_context = context_builder.hydrate(
                base_context=market_context, 
                context_skeleton=context_skeleton
            )

            print(" [MARKET] -> Evaluating Playbook")
            
            # 3. Evaluate Playbook
            playbook_results = playbook.evaluate(full_context)
            
            print(" [MARKET] -> Determining Triggers")
            
            # 4. Determine triggers
            entry_triggers = playbook_results.get(RuleCategory.ENTRY, [])
            rule_result = len(entry_triggers) > 0

            triggered_entry_labels = [
                rule.name
                for rule in playbook.rules
                if rule.category == RuleCategory.ENTRY and (rule.id or rule.name) in entry_triggers
            ]
            if not triggered_entry_labels and entry_triggers:
                triggered_entry_labels = [str(trigger) for trigger in entry_triggers]

            # 4b. Map all rules in the playbook to their boolean trigger status
            rule_evaluations = {}
            for rule in playbook.rules:
                result_keys = playbook_results.get(rule.category, [])
                rule_evaluations[rule.name] = (rule.id or rule.name) in result_keys

            print(" [MARKET] -> Checking User Action")
            
            # 5. User Action & Deviation
            user_action_bool = await state.get_and_reset_user_action()
            deviation = rule_result != user_action_bool

            print(" [MARKET] -> Preparing Output Payload")
            
            # 6. Output Payload
            rule_summary = ", ".join(triggered_entry_labels) if triggered_entry_labels else "No entry rule triggered"
            output_payload = {
                "timestamp": market_context.get("current_time") or market_context.get("timestamp"),
                "price": market_context.get("price"),
                "playbook_id": playbook_id,
                "session_id": session_id,
                "user_id": user_id,
                "rule": rule_summary,
                "rule_triggered": rule_result,
                "triggered_entries": triggered_entry_labels,
                "rule_evaluations": rule_evaluations,
                "action": user_action_bool,
                "deviation": deviation
            }
            
            price_display = output_payload["price"] if output_payload["price"] is not None else "N/A"
            print(
                f"TIME: {output_payload['timestamp']} | "
                f"PRICE: {str(price_display):<8} | "
                f"RULE: {str(rule_result):<5} | "
                f"TRIGGERS: {entry_triggers} | "
                f"ACTION: {str(user_action_bool):<5} | "
                f"DEVIATION: {str(deviation)}"
            )
            
            print(" [MARKET] -> Broadcasting Payload")

            evaluation_tick += 1
            await output_registry.broadcast(output_payload, user_id=user_id, session_id=session_id)

            if session_id:
                await persist_backend_session_signal(
                    session_id=session_id,
                    payload=output_payload,
                    tick=evaluation_tick,
                )
                        
        except Exception as e:
            import traceback
            print(f" [MARKET ENGINE CRITICAL ERROR] Failed during market_handler: {e}")
            traceback.print_exc()

    await client.listen(market_handler)


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
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(fetch_url, headers={"accept": "application/json"}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    prompt_text = data.get("original_nl_input") or data.get("rule_text", "")
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

    # 2. Parse the rule using the LLM
    llm_client = OpenAILLMClient(model="gpt-4.1")
    parser = RuleParser(llm_client, category=RuleCategory.ENTRY)
    
    print(f"[ENGINE] Parsing rule playbook...")
    try:
        playbook, context_skeleton = await asyncio.to_thread(parser.parse, prompt_text)
        if context_skeleton is None:
            print("[ENGINE ERROR] Parser returned no context skeleton.")
            await patch_backend_playbook(playbook_id, {"generation_status": "FAILED"})
            return
        skeleton_dict = dict(context_skeleton) if not hasattr(context_skeleton, "model_dump") else context_skeleton.model_dump()
        
        # 3. Persist the parsed rules/conditions to backend DB
        from populate_tables import populate_playbook_tables
        try:
            await populate_playbook_tables(playbook_id, playbook)
        except Exception as pop_err:
            print(f"[ENGINE WARNING] DB Population failed: {pop_err}")
            await patch_backend_playbook(playbook_id, {"generation_status": "FAILED"})
            return

    except Exception as e:
        print(f"[ENGINE ERROR] Failed to parse playbook: {e}")
        await patch_backend_playbook(playbook_id, {"generation_status": "FAILED"})
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
        
    patch_data = {"context": skeleton_dict}
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
    print(f"\n[ENGINE] Starting execution for Playbook: {playbook_id}")

    fetch_url = build_backend_http_url(f"/playbooks/{playbook_id}")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(fetch_url, headers={"accept": "application/json"}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    user_id = user_id or data.get("user_id")
                else:
                    print(f"[ENGINE ERROR] Failed to fetch playbook to execute. Status: {resp.status}")
                    return None
        except Exception as e:
            print(f"[ENGINE ERROR] Could not reach Supabase for execute: {e}")
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
    context_skeleton = ContextSkeletonSchema(**safe_context_data)

    # 3. Spin up trading engine loops
    alpaca_provider = AlpacaAccountProvider(
        api_key=os.getenv("API_KEY"),
        api_secret=os.getenv("SECRET_KEY"),
        paper=True
    )
    
    context_builder = ContextBuilder(
        account_provider=alpaca_provider,
        user_action_provider=None,
        global_account_fields=GLOBAL_ACCOUNT_FIELDS
    )

    user_ws_url = build_backend_ws_url("/ws/user-activity", user_id=user_id, session_id=session_id)
    market_ws_url = build_backend_ws_url("/ws/market-state", user_id=user_id, session_id=session_id)
    
    user_ws = WebSocketClient(user_ws_url)
    state = EngineState()

    async def handle_user_activity(msg: str):
        await user_activity_handler(msg, state)

    print("[ENGINE] Starting trading WebSockets in background...")
    task_user = asyncio.create_task(user_ws.listen(handle_user_activity))
    task_market = asyncio.create_task(run_market_engine(
        market_ws_url, 
        playbook, 
        context_builder, 
        context_skeleton,
        output_registry,
        state,
        playbook_id,
        session_id=session_id,
        user_id=user_id,
    ))

    # Return the tasks so main.py can manage/cancel them later if needed
    return task_user, task_market
