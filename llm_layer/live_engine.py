# llm_layer/live_engine.py
import sys
import os
import pprint
import asyncio
import json
import pandas as pd
from typing import Dict, Any, Set
from aiohttp import web, WSMsgType

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import ContextBuilder, Primitive, PrimitiveRegistry, RuleBlock, RuleCategory
from primitives import (
    comparison_evaluator,
    temporal_gate_evaluator,
    account_comparison_evaluator,
    set_membership_evaluator,
    rate_limit_evaluator,
    accumulation_evaluator,
    sequence_evaluator
)
from account_providers import AlpacaAccountProvider
from account_validation import GLOBAL_ACCOUNT_FIELDS
from rule_parser import RuleParser
from llm_layer.openai_client import OpenAILLMClient
from llm_layer.data_ingestion import WebSocketClient
from dotenv import load_dotenv

load_dotenv("../.env")

# -----------------------------
# Configuration & Registries
# -----------------------------

# Register Primitives (subset needed for the example)
if "comparison" not in PrimitiveRegistry._registry:
    PrimitiveRegistry.register(
        Primitive("comparison", comparison_evaluator, required_context=["price"])
    )
if "temporal_gate" not in PrimitiveRegistry._registry:
    PrimitiveRegistry.register(
        Primitive("temporal_gate", temporal_gate_evaluator, required_context=["current_time"])
    )
if "account_comparison" not in PrimitiveRegistry._registry:
    PrimitiveRegistry.register(
        Primitive("account_comparison", account_comparison_evaluator)
    )
if "set_membership" not in PrimitiveRegistry._registry:
    PrimitiveRegistry.register(
        Primitive("set_membership", set_membership_evaluator)
    )
if "rate_limit" not in PrimitiveRegistry._registry:
    PrimitiveRegistry.register(
        Primitive("rate_limit", rate_limit_evaluator)
    )
if "accumulation" not in PrimitiveRegistry._registry:
    PrimitiveRegistry.register(
        Primitive("accumulation", accumulation_evaluator)
    )
if "sequence" not in PrimitiveRegistry._registry:
    PrimitiveRegistry.register(
        Primitive("sequence", sequence_evaluator)
    )

# -----------------------------
# Shared State
# -----------------------------
class EngineState:
    def __init__(self):
        self.user_has_acted = False
        self.lock = asyncio.Lock()

    async def set_user_action(self, acted: bool):
        async with self.lock:
            self.user_has_acted = acted
    
    async def get_and_reset_user_action(self) -> bool:
        async with self.lock:
            acted = self.user_has_acted
            self.user_has_acted = False  # Reset after reading
            return acted

state = EngineState()

# Global set of connected clients for result broadcasting
connected_clients: Set[web.WebSocketResponse] = set()

# -----------------------------
# WebSocket Handlers
# -----------------------------

async def user_activity_handler(msg: str):
    """
    Listens for user activity on the websocket.
    Any message received here counts as a 'user action' for the current interval.
    """
    print(f" [USER RAW MSG] {msg}")
    try:
        data = json.loads(msg)
        # In a real scenario, we might check data['alpaca_event_type'] == 'fill' or similar.
        # For now, per instructions: "when a user action does come true"
        # We assume availability of this message implies an action occurred.
        print(f" [USER ACTION RECEIVED] {data.get('activity_id', 'unknown_id')}")
        await state.set_user_action(True)
    except json.JSONDecodeError:
        print(f" [USER ACTION ERROR] Invalid JSON: {msg[:50]}...")
    except Exception as e:
        print(f" [USER ACTION ERROR] {e}")

async def run_market_engine(
    ws_url: str,
    playbook: Any,
    context_builder: ContextBuilder,
    context_skeleton: Any
):
    print("\n--- FRONTEND CONTEXT REQUEST SKELETON ---")
    if hasattr(context_skeleton, "model_dump_json"):
        print(context_skeleton.model_dump_json(indent=2))
    else:
        print(json.dumps(dict(context_skeleton), indent=2))
    print("-----------------------------------------\n")

    client = WebSocketClient(ws_url)
    
    print(f" [MARKET] Connecting to {ws_url}...")
    
    # Send subscription for TA-lib metrics
    if context_skeleton.ta_lib_metrics:
        sub_msg = {
            "action": "subscribe",
            "symbol": context_skeleton.symbol,
            "ta_lib_metrics": [m.model_dump() for m in context_skeleton.ta_lib_metrics]
        }
        await client.send(sub_msg)
        print(f" [MARKET] Requested TA-Lib metrics: {[m.name for m in context_skeleton.ta_lib_metrics]}")
    
    async def market_handler(msg: str):
        try:
            data = json.loads(msg)
            
            # 1. Build Base Context from Market Data
            market_context = {}
            if "price" in data:
                market_context["price"] = data["price"]
            if "current_time" in data:
                market_context["current_time"] = data["current_time"]
            if "symbol" in data:
                market_context["symbol"] = data["symbol"]
            
            # Retrieve injected TA-Lib metrics from the data stream and add to base context
            if context_skeleton.ta_lib_metrics:
                for metric in context_skeleton.ta_lib_metrics:
                    # Construct matching key constraint used by parser e.g., 'RSI_14' or 'MACD'
                    key = f"{metric.name}_{metric.timeperiod}" if metric.timeperiod else metric.name
                    if key in data:
                        market_context[key] = data[key]

            # 2. Hydrate Full Context (fetches account data if needed)
            full_context = context_builder.hydrate(
                base_context=market_context, 
                context_skeleton=context_skeleton
            )

            # 3. Evaluate Playbook
            # playbook_results returns Dict[RuleCategory, List[str]]
            playbook_results = playbook.evaluate(full_context)
            
            # Since we only really care about the simulation of Entry for this loop
            entry_triggers = playbook_results.get(RuleCategory.ENTRY, [])
            rule_result = len(entry_triggers) > 0

            # 4. Get and Reset User Action State
            # This captures if the user acted since the last evaluation
            user_action_bool = await state.get_and_reset_user_action()

            # 5. Calculate Deviation
            # For ENTRY rules, they behave as permissive guardrails:
            # - Action taken when rule is FALSE = Deviation (violated restriction)
            # - Action NOT taken when rule is TRUE = NO Deviation (just passing on a valid setup)
            deviation = user_action_bool and not rule_result

            # 6. Output Result
            output_payload = {
                "timestamp": market_context.get("current_time"),
                "price": market_context.get("price"),
                "rule_triggered": rule_result,
                "triggered_entries": entry_triggers,
                "action": user_action_bool,
                "deviation": deviation
            }
            
            # Print to console
            print(
                f"TIME: {output_payload['timestamp']} | "
                f"PRICE: {output_payload['price']:<8} | "
                f"RULE: {str(rule_result):<5} | "
                f"TRIGGERS: {entry_triggers} | "
                f"ACTION: {str(user_action_bool):<5} | "
                f"DEVIATION: {str(deviation)}"
            )
            
            # Broadcast to all connected WebSocket clients
            if connected_clients:
                # Iterate over a copy to avoid modification issues during iteration
                for ws in list(connected_clients):
                    try:
                        await ws.send_json(output_payload)
                    except Exception as send_err:
                        print(f" [RESULT STREAM ERROR] {send_err}")
                        # Clean up dead connections lazily or rely on the handler's finally block
                        
        except Exception as e:
            print(f" [MARKET ENGINE ERROR] {e}")

    await client.listen(market_handler)


# -----------------------------
# Main Setup
# -----------------------------
async def handle_health(request):
    return web.Response(text="OK")

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    print(" [WEBSOCKET] Client connected")
    connected_clients.add(ws)
    
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                # Optional: Handle incoming messages from clients if needed
                pass
            elif msg.type == WSMsgType.ERROR:
                print(f" [WEBSOCKET] Connection closed with exception {ws.exception()}")
    finally:
        connected_clients.remove(ws)
        print(" [WEBSOCKET] Client disconnected")
    
    return ws

async def start_web_server():
    app = web.Application()
    app.add_routes([
        web.get('/', handle_health),
        web.get('/health', handle_health),
        web.get('/ws/engine-output', websocket_handler)
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f" [SERVER] Listening on port {port}")
    return runner

async def main():
    # 1. Hardware/Provider Setup
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

    # 2. Define Rule (Hardcoded or Parsed)
    # Using a simple hardcoded rule for "Price > X" to test easily
    # Or valid LLM parsing if configured. Let's use the parser for realism.
    llm_client = OpenAILLMClient(model="gpt-4.1")
    parser = RuleParser(llm_client, category=RuleCategory.ENTRY)
    
    user_rule_text = """
1. Setup Logic (Deterministic Inputs)

Derived State:
	•	Session VWAP (UTC daily reset)
	•	20-period EMA
	•	14-period ATR
	•	Rolling volatility regime
	•	Daily realized PnL

⸻

Long Setup

Conditions must ALL be true:
	1.	Price < VWAP − 1.5 × ATR
	2.	EMA slope > 0
	3.	5-min close back above prior candle high
	4.	Not within 10 minutes of previous stop

⸻

Short Setup
	1.	Price > VWAP + 1.5 × ATR
	2.	EMA slope < 0
	3.	5-min close below prior candle low
	4.	Not within 10 minutes of previous stop

⸻

2. Entry Rules
	•	Market order at next candle open.
	•	Max 1 position at a time.
	•	No pyramiding.
	•	No flipping within 5 minutes.

⸻

3. Risk Model

Stop: 1 ATR
Target: 2 ATR
Trailing stop activates at +1R
Max daily loss: 3R
Max 5 trades per UTC day
Position size: 1% account risk per trade

⸻

4. Meta Discipline Rules (Where TMOM Shines)

Hard Constraints:
	•	Block trade if daily loss ≥ 3R
	•	Block if > 5 trades
	•	Block if position size > 1% risk

Soft Guardrails:
	•	Warn if trade taken within 3 minutes of prior close
	•	Warn if volatility > 95th percentile
	•	Require justification if third consecutive loss

Cooldown:
	•	10 minutes after stop loss
	•	30 minutes after 2 consecutive losses
"""
    print(f"Initializing Engine with Rule: '{user_rule_text[:50]}...'")
    
    try:
        playbook, context_skeleton = parser.parse(user_rule_text)
    except Exception as e:
        print(f"Failed to parse playbook: {e}")
        return

    # 3. Start Websockets
    user_ws_url = "wss://tmom-app-backend.onrender.com/ws/user-activity"
    market_ws_url = "wss://tmom-app-backend.onrender.com/ws/market-state"
    # Result URL is now hosted locally

    user_ws = WebSocketClient(user_ws_url)

    # Create tasks
    print("Starting listeners...")
    
    # Start Web Server (Health + WebSocket)
    web_runner = await start_web_server()
    
    task_user = asyncio.create_task(user_ws.listen(user_activity_handler))

    # We choose an ENTRY rule to drive the market engine for this example, 
    # but in a full system we'd evaluate the entire playbook.
    entry_rules = playbook.get_rules_by_category(RuleCategory.ENTRY)

    if not entry_rules:
        print("No ENTRY rule found in playbook.")
        return
    
    # For the simulation/live engine, we pass the entire playbook.
    task_market = asyncio.create_task(run_market_engine(
        market_ws_url, 
        playbook, 
        context_builder, 
        context_skeleton
    ))


    # Keep alive
    try:
        await asyncio.gather(task_user, task_market)
    finally:
        await web_runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nEngine stopped.")
