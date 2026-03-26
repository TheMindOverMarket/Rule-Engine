import os
import json
import asyncio
import aiohttp
from typing import Any, Dict, Optional, Set
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

# Global set of connected clients for local WebSocket broadcasting
# (We might need to pass this from main.py, or define it here if execution_engine manages the broadcast)
connected_clients: Set[Any] = set()

# Mock State Manager
class EngineState:
    def __init__(self):
        self.user_took_action = False
    
    async def get_and_reset_user_action(self) -> bool:
        val = self.user_took_action
        self.user_took_action = False
        return val

state = EngineState()

GLOBAL_ACCOUNT_FIELDS = ["equity", "buying_power", "cash", "daytrade_count", "open_positions"]

async def user_activity_handler(msg: str):
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
    clients_set: Set[Any]
):
    print("\n--- FRONTEND CONTEXT REQUEST SKELETON ---")
    if hasattr(context_skeleton, "model_dump_json"):
        print(context_skeleton.model_dump_json(indent=2))
    else:
        print(json.dumps(dict(context_skeleton), indent=2))
    print("-----------------------------------------\n")

    client = WebSocketClient(ws_url)
    print(f" [MARKET] Connecting to {ws_url}...")
    
    async def market_handler(msg: str):
        try:
            print(" [MARKET] -> Loading JSON message")
            data = json.loads(msg)
            
            # Explicitly catch backend unauthorized JSON payloads pushed over an open socket.
            if isinstance(data, dict) and data.get("message") == "unauthorized.":
                 print(f" [MARKET ENGINE ERROR] Received explicit 'unauthorized.' payload from backend stream ({ws_url})")
                 print(f"                       Full payload dumped: {data}")
                 return

            print(" [MARKET] -> Extracting Base Context")
            
            # 1. Build Base Context from Market Data
            market_context = {}
            if "price" in data:
                market_context["price"] = data["price"]
            if "current_time" in data:
                market_context["current_time"] = data["current_time"]
            if "symbol" in data:
                market_context["symbol"] = data["symbol"]
            
            print(" [MARKET] -> Extracting TA-Lib Metrics")
            
            # Retrieve injected TA-Lib metrics from the data stream and add to base context
            if context_skeleton.ta_lib_metrics:
                for metric in context_skeleton.ta_lib_metrics:
                    key = f"{metric.name}_{metric.timeperiod}" if metric.timeperiod else metric.name
                    if key in data:
                        market_context[key] = data[key]

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

            # 4b. Map all rules in the playbook to their boolean trigger status
            rule_evaluations = {}
            for rule in playbook.rules:
                rule_key = rule.id or rule.name
                rule_evaluations[rule_key] = rule_key in playbook_results.get(rule.category, [])

            print(" [MARKET] -> Checking User Action")
            
            # 5. User Action & Deviation
            user_action_bool = await state.get_and_reset_user_action()
            deviation = rule_result != user_action_bool

            print(" [MARKET] -> Preparing Output Payload")
            
            # 6. Output Payload
            output_payload = {
                "timestamp": market_context.get("current_time"),
                "price": market_context.get("price"),
                "rule_triggered": rule_result,
                "triggered_entries": entry_triggers,
                "rule_evaluations": rule_evaluations,  # Tell frontend state of ALL rules
                "action": user_action_bool,
                "deviation": deviation
            }
            
            print(
                f"TIME: {output_payload['timestamp']} | "
                f"PRICE: {output_payload['price']:<8} | "
                f"RULE: {str(rule_result):<5} | "
                f"TRIGGERS: {entry_triggers} | "
                f"ACTION: {str(user_action_bool):<5} | "
                f"DEVIATION: {str(deviation)}"
            )
            
            print(" [MARKET] -> Broadcasting Payload")
            
            # Broadcast to all connected WebSocket clients
            if clients_set:
                for ws in list(clients_set):
                    try:
                        await ws.send_json(output_payload)
                    except Exception as send_err:
                        print(f" [RESULT STREAM ERROR] {send_err}")
                        
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
    fetch_url = f"https://tmom-app-backend.onrender.com/playbooks/{playbook_id}"
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
        return

    # 2. Parse the rule using the LLM
    llm_client = OpenAILLMClient(model="gpt-4.1")
    parser = RuleParser(llm_client, category=RuleCategory.ENTRY)
    
    print(f"[ENGINE] Parsing rule playbook...")
    try:
        playbook, context_skeleton = parser.parse(prompt_text)
        skeleton_dict = dict(context_skeleton) if not hasattr(context_skeleton, "model_dump") else context_skeleton.model_dump()
        
        # 3. Persist the parsed rules/conditions to backend DB
        from populate_tables import populate_playbook_tables
        try:
            await populate_playbook_tables(playbook_id, playbook)
        except Exception as pop_err:
            print(f"[ENGINE WARNING] DB Population failed: {pop_err}")
            
    except Exception as e:
        print(f"[ENGINE ERROR] Failed to parse playbook: {e}")
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

    patch_url = f"https://tmom-app-backend.onrender.com/playbooks/{playbook_id}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.patch(
                patch_url,
                json=patch_data,
                headers={"accept": "application/json"}
            ) as patch_resp:
                if patch_resp.status in (200, 201, 204):
                    print("[ENGINE] Successfully patched Context Skeleton and Compiled Rules to database.")
                else:
                    err_text = await patch_resp.text()
                    print(f"[ENGINE WARNING] Failed to patch context. Status: {patch_resp.status}, Response: {err_text}")
        except Exception as e:
            print(f"[ENGINE WARNING] Could not patch Supabase: {e}")


async def execute_playbook(playbook_id: str, clients_set: Set[Any]):
    """
    Execution flow:
    1. Fetch compiled Context Skeleton & Rules from Supabase
    2. Reconstruct Playbook object in memory
    3. Notify frontend to start streaming
    4. Spin up the trading engine loops
    """
    print(f"\n[ENGINE] Starting execution for Playbook: {playbook_id}")

    fetch_url = f"https://tmom-app-backend.onrender.com/playbooks/{playbook_id}"
    user_id = None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(fetch_url, headers={"accept": "application/json"}) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    user_id = data.get("user_id")
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

    # 3. Notify frontend's setup stream endpoint
    notify_url = "https://tmom-app-backend.onrender.com/start_streams_creation"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                notify_url,
                json={"user_id": user_id, "playbook_id": playbook_id},
                headers={
                    "accept": "application/json",
                    "Content-Type": "application/json"
                }
            ) as notify_resp:
                print(f"[ENGINE] Notified frontend setup stream. Status: {notify_resp.status}")
        except Exception as e:
            print(f"[ENGINE WARNING] Could not notify frontend setup stream: {e}")

    # 4. Spin up trading engine loops
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

    user_ws_url = "wss://tmom-app-backend.onrender.com/ws/user-activity"
    market_ws_url = "wss://tmom-app-backend.onrender.com/ws/market-state"
    
    user_ws = WebSocketClient(user_ws_url)

    print("[ENGINE] Starting trading WebSockets in background...")
    task_user = asyncio.create_task(user_ws.listen(user_activity_handler))
    task_market = asyncio.create_task(run_market_engine(
        market_ws_url, 
        playbook, 
        context_builder, 
        context_skeleton,
        clients_set
    ))

    # Return the tasks so main.py can manage/cancel them later if needed
    return task_user, task_market
