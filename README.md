# Rule-Engine

> The brain of the TMOM platform. This service compiles natural-language trading strategies into deterministic evaluation logic, monitors live market feeds, and generates AI-powered deviation explanations.

**Runtime:** Python 3.11+ · FastAPI · OpenAI GPT-4 · Alpaca WebSocket  
**Deployment:** Render (`https://rule-engine-rcg9.onrender.com`)  
**Local Port:** `8001`

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture Overview](#architecture-overview)
3. [Module Reference](#module-reference)
   - [main.py — API & Orchestration](#mainpy--api--orchestration)
   - [engine.py — Data Structures](#enginepy--data-structures)
   - [primitives.py — Evaluation Primitives](#primitivespy--evaluation-primitives)
   - [logic_adherence.py — Deviation Classification](#logic_adherencepy--deviation-classification)
   - [execution_engine.py — Live Session Manager](#execution_enginepy--live-session-manager)
   - [llm_layer/ — AI Integration](#llm_layer--ai-integration)
   - [network/ — WebSocket Infrastructure](#network--websocket-infrastructure)
   - [broker/ — Brokerage Integration](#broker--brokerage-integration)
   - [populate_tables.py — Database Sync](#populate_tablespy--database-sync)
4. [API Reference](#api-reference)
5. [WebSocket Outputs](#websocket-outputs)
6. [Configuration](#configuration)
7. [Key Design Decisions](#key-design-decisions)
8. [Debugging Guide](#debugging-guide)

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env  # Then fill in API_KEY, SECRET_KEY, OPENAI_KEY

# 3. Run
uvicorn main:app --reload --port 8001
```

**Dependencies on other services:**
- The Backend must be running at `TMOM_BACKEND_BASE_URL` (default: `http://localhost:8000`)
- An OpenAI API key is required for LLM compilation and reasoning
- Alpaca API credentials are required for market data and account snapshots

---

## Architecture Overview

```
                    ┌──────────────────────────┐
                    │        main.py           │
                    │  FastAPI Entry Point     │
                    │  /api/rules/* endpoints  │
                    └────┬─────────┬───────────┘
                         │         │
              ┌──────────┘         └──────────┐
              ▼                               ▼
    ┌──────────────────┐           ┌──────────────────────┐
    │  LLM Layer       │           │  Execution Engine     │
    │  (Compilation)   │           │  (Live Evaluation)    │
    │                  │           │                       │
    │  rule_parser.py  │           │  execution_engine.py  │
    │  reasoner.py     │           │  logic_adherence.py   │
    │  openai_client.py│           │  engine.py            │
    └──────────────────┘           │  primitives.py        │
                                   └───────┬───────────────┘
                                           │
                              ┌────────────┼────────────┐
                              ▼            ▼            ▼
                    ┌──────────┐  ┌──────────┐  ┌──────────┐
                    │ Market   │  │ Account  │  │ Backend  │
                    │ Data Hub │  │ Provider │  │ API      │
                    │ (WS)    │  │ (Alpaca) │  │ (HTTP)   │
                    └──────────┘  └──────────┘  └──────────┘
```

The Rule Engine operates in two distinct modes:

### 1. Compilation Mode (`/api/rules/compile`)
Triggered when a user creates or updates a playbook. The LLM layer parses the natural language into a structured `Playbook` object, then persists the compiled rules back to the Backend database.

### 2. Execution Mode (`/api/rules/execute`)
Triggered when a user starts a live session. The execution engine subscribes to market data, evaluates rules on every tick, broadcasts results via WebSocket, and persists events to the database.

---

## Module Reference

### `main.py` — API & Orchestration

**Location:** `/Rule-Engine/main.py`  
**Role:** FastAPI application entry point. All HTTP endpoints and the global lifecycle are defined here.

#### Lifecycle Management

```python
@app.on_event("startup")
```

On startup, the engine:
1. **Auto-recovers** any sessions that were marked `STARTED` in the database (queries `GET /sessions/?status=STARTED`).
2. Spawns background tasks for each recovered session to resume evaluation.
3. Starts a **heartbeat task** that periodically pings the Backend to keep Render instances alive.

```python
@app.on_event("shutdown")
```

On shutdown:
1. Cancels all active execution tasks.
2. Unsubscribes from all market data hubs.
3. Closes the `EngineOutputRegistry` WebSocket connections.

#### Key Global State

| Variable | Type | Purpose |
| :------- | :--- | :------ |
| `_active_tasks` | `Dict[str, asyncio.Task]` | Maps `playbook_id` → running evaluation task |
| `_compiled_playbooks` | `Dict[str, Playbook]` | In-memory cache of compiled playbooks |
| `engine_output_registry` | `EngineOutputRegistry` | WebSocket broadcaster for evaluation results |

#### Concurrency Model

- Each active session runs as a separate `asyncio.Task`.
- The `/api/rules/execute` endpoint creates the task; `/api/rules/stop` cancels it.
- The `compile` endpoint runs synchronously (LLM calls are awaited) but within a background task dispatched by the Backend.

---

### `engine.py` — Data Structures

**Location:** `/Rule-Engine/engine.py`  
**Role:** Defines the core data model for compiled playbooks.

#### Class Hierarchy

```
Playbook
├── rules: List[RuleBlock]
│   ├── id: str
│   ├── name: str
│   ├── category: RuleCategory (ENTRY, RISK, META, SETUP, etc.)
│   ├── conditions: dict  ←  { "all": ["ext_0", "ext_1"], "any": [...] }
│   └── extensions: Dict[str, Extension]
│       ├── ext_0:
│       │   ├── primitive: str  ←  "comparison_evaluator"
│       │   └── params: dict    ←  { "left": "price", "op": "<", "right": "vwap" }
│       └── ext_1: ...
├── ta_lib_metrics: List[dict]      ←  Indicators needed (e.g., EMA, ATR, VWAP)
├── market_data: List[dict]         ←  Raw market data fields needed
└── context: dict                   ←  Symbol, market metadata
```

#### RuleCategory Enum

| Value | Meaning |
| :---- | :------ |
| `ENTRY` | Conditions for entering a position |
| `RISK` | Stop loss, position sizing, max daily loss |
| `META` | Discipline rules (cooldowns, blocking) |
| `SETUP` | Pre-conditions (market state must be true before entry) |

#### Condition Tree Evaluation

The `conditions` field is a nested dict:

```json
{
  "all": [
    "ext_0",
    { "any": ["ext_1", "ext_2"] }
  ]
}
```

This means: `ext_0 AND (ext_1 OR ext_2)`.

The `evaluate_conditions()` function in `engine.py` walks this tree recursively:
- `"all"` → All children must be `True` (AND)
- `"any"` → At least one child must be `True` (OR)
- `"none"` → No children may be `True` (NOR)
- String leaf → Looks up the extension by ID and evaluates its primitive

---

### `primitives.py` — Evaluation Primitives

**Location:** `/Rule-Engine/primitives.py`  
**Role:** Contains the registered evaluation functions ("primitives") that perform the actual logical checks.

#### Registration Pattern

```python
PRIMITIVE_REGISTRY = {}

def register_primitive(name: str):
    def decorator(fn):
        PRIMITIVE_REGISTRY[name] = fn
        return fn
    return decorator

@register_primitive("comparison_evaluator")
def comparison_evaluator(params: dict, context: dict) -> bool:
    ...
```

Each primitive receives:
- `params`: The extension's parameters (e.g., `{"left": "price", "op": "<", "right": "vwap - 1.5 * atr"}`)
- `context`: The current market/account state (e.g., `{"price": 97000, "vwap": 96800, "atr": 150}`)

#### Primitive Reference

##### `comparison_evaluator`
Compares a left-hand metric to a right-hand expression.

```python
# params: { "left": "price", "op": "<", "right": "vwap - 1.5 * atr" }
# context: { "price": 97000, "vwap": 96800, "atr": 150 }
# Result: 97000 < 96800 - 225 → 97000 < 96575 → False
```

**Supported operators:** `<`, `>`, `<=`, `>=`, `==`, `!=`

The `right` value can be a **mathematical expression** involving context variables. It is evaluated using a **safe eval** mechanism that:
1. Replaces variable names with their context values.
2. Uses `ast.literal_eval` or a restricted evaluator (no `exec`, `import`, etc.).
3. Falls back gracefully if variables are missing from context.

##### `rate_limit_evaluator`
Enforces frequency limits (e.g., "max 5 trades per day").

```python
# params: { "metric": "trade_count", "max": 5, "window": "1d" }
```

##### `set_membership_evaluator`
Checks if a value is in or not in a set.

```python
# params: { "field": "symbol", "op": "in", "set": ["BTC/USD", "ETH/USD"] }
```

##### `temporal_gate_evaluator`
Time-based gates for cooldowns.

```python
# params: { "start_time": "last_stop_loss_time", "cooldown_end": 600 }
# Checks: current_time - last_stop_loss_time > 600 seconds
```

##### `accumulation_evaluator`
Tracks cumulative values over a window.

```python
# params: { "field": "daily_pnl", "op": ">=", "threshold": -3.0 }
```

##### `account_comparison_evaluator`
Compares against live brokerage account data (fetched from Alpaca).

```python
# params: { "field": "buying_power", "op": ">", "right": "0" }
```

---

### `logic_adherence.py` — Deviation Classification

**Location:** `/Rule-Engine/logic_adherence.py`  
**Role:** The core evaluation orchestrator. Takes a `Playbook` + `context` and returns a complete adherence assessment.

#### Main Function: `build_logic_adherence_payload()`

```python
def build_logic_adherence_payload(
    playbook: Playbook,
    context: dict,
    action_event: dict = None  # Optional: user trade to validate
) -> dict:
```

**Returns:**

```json
{
  "playbook_id": "...",
  "session_id": "...",
  "timestamp": "...",
  "price": 97000.5,
  "rule_status": {
    "rule_id_1": true,
    "rule_id_2": false
  },
  "deviation": true,
  "deviation_true": ["Rule: Max Daily Loss exceeded"],
  "deviation_false": [],
  "action": false,
  "rule_evaluations": {
    "rule_id_1": { "ext_0": true, "ext_1": true },
    "rule_id_2": { "ext_0": true, "ext_1": false }
  }
}
```

#### Deviation Types

| Field | Type | Meaning |
| :---- | :--- | :------ |
| `deviation` | `bool` | Any rule violated? |
| `deviation_true` | `list[str]` | Names of violated rules |
| `deviation_false` | `list[str]` | Names of compliant rules |
| `action` | `bool` | Was this triggered by a user action (trade)? |
| `rule_status` | `dict` | Per-rule pass/fail map |
| `rule_evaluations` | `dict` | Per-extension evaluation results |

#### ACTION vs. STATE Deviations

```
STATE Deviation:
  - Triggered every tick
  - Example: "Price is below stop loss threshold"
  - Does NOT require a user action

ACTION Deviation:
  - Triggered when a trade fill arrives
  - Example: "User bought when entry conditions were not met"
  - Requires matching the trade against entry rules
```

The engine tracks `_last_deviation_set` to avoid re-broadcasting the same state deviations.

---

### `execution_engine.py` — Live Session Manager

**Location:** `/Rule-Engine/execution_engine.py`  
**Role:** Manages the complete lifecycle of a live trading session.

#### Session Lifecycle

```python
async def run_session(playbook_id, session_id, user_id, symbol):
    # 1. Fetch compiled playbook from Backend
    # 2. Subscribe to market data (MarketDataHub)
    # 3. Subscribe to user activity (trade fills)
    # 4. Start persistence_worker (background queue consumer)
    # 5. Enter main evaluation loop
    #    - On each tick: build_logic_adherence_payload()
    #    - Broadcast result via EngineOutputRegistry
    #    - Enqueue persistence event
    # 6. On cancellation: cleanup and unsubscribe
```

#### Key Internal Components

##### EngineState

Tracks the session's mutable state:

```python
class EngineState:
    tick_count: int
    last_price: float
    last_deviation_set: set
    sticky_deviations: dict       # ACTION deviations that persist across ticks
    pending_ai_reasoning: dict    # Deviations awaiting AI explanation
```

##### Persistence Worker

A background `asyncio.Task` that:
1. Reads from an `asyncio.Queue` (unbounded consumer, bounded producer).
2. Batches events and POSTs them to `Backend /sessions/{id}/events`.
3. If a new deviation is detected, triggers `DeviationReasoner.explain_deviation()`.
4. PATCHes the event with the AI reasoning once generated.
5. Broadcasts the updated event via WebSocket.

**Why a queue?** During high-volatility periods, the engine can process hundreds of ticks per second. Without the queue, each tick would trigger a synchronous HTTP POST, creating backpressure. The queue allows the engine to drop ticks under load (via `put_nowait()` with a bounded queue) rather than slowing down evaluation.

##### EngineOutputRegistry

A WebSocket manager that:
- Accepts connections with `session_id` and/or `user_id` query params.
- Broadcasts evaluation payloads to all connected frontends.
- Handles disconnections and reconnections gracefully.

#### Tick Processing Pipeline

```
Market Tick Arrives
    │
    ▼ _handle_market_tick(data)
    │
    ├─ 1. Build context dict from tick data
    │     { price, high, low, vwap, ema_20, atr_14, ... }
    │
    ├─ 2. Hydrate account data (AlpacaAccountProvider, 5s TTL cache)
    │
    ├─ 3. build_logic_adherence_payload(playbook, context)
    │
    ├─ 4. Check for NEW deviations (compare vs _last_deviation_set)
    │     - New deviations get { ai_reasoning: "GENERATING..." } placeholder
    │
    ├─ 5. Merge sticky ACTION deviations into output
    │
    ├─ 6. Broadcast via EngineOutputRegistry.broadcast()
    │
    └─ 7. Enqueue for persistence (persistence_worker)
```

---

### `llm_layer/` — AI Integration

#### `rule_parser.py` — Strategy Compilation

**Class:** `RuleParser`

```python
class RuleParser:
    async def parse(self, nl_input: str, symbol: str, context: dict) -> Playbook:
        # 1. Build system prompt with JSON schema + available primitives
        # 2. Call OpenAI GPT-4 with the user's NL strategy
        # 3. Parse JSON response
        # 4. Validate against schema
        # 5. If invalid: enter REPAIR LOOP (up to 3 attempts)
        #    - Send validation errors back to LLM
        #    - LLM corrects and re-generates
        # 6. Hydrate into Playbook object
        # 7. Return
```

**Key Design:** The system prompt includes:
- The complete JSON schema expected
- A list of all available primitives with parameter documentation
- Examples of correct outputs
- The user's market context (symbol, available indicators)

**Repair Loop:** If the LLM returns invalid JSON (missing fields, wrong types), the parser:
1. Catches the validation error.
2. Formats it as a user-friendly message.
3. Sends `[SYSTEM: Your output had these errors: ...]` back to the LLM.
4. The LLM attempts to fix its output.
5. This repeats up to 3 times before failing.

#### `reasoner.py` — Deviation Explanations

**Class:** `DeviationReasoner`

```python
class DeviationReasoner:
    async def explain_deviation(self, playbook_text: str, deviation_data: dict) -> str:
        # Sends the original NL strategy + deviation context to GPT-4
        # Returns a 1-2 sentence natural language explanation
        # Example: "The trader entered a long position while the EMA slope was
        #           negative, violating the Long Setup condition #2."

    async def session_report_card(self, playbook_text: str, events: list) -> str:
        # Generates a full session summary
        # Includes: adherence rate, key violations, recommendations
```

**Critical Rule:** The reasoner is instructed to **only reference violations that actually appear in the `deviation_true` array**. This prevents hallucinated explanations about rules that were actually passing.

#### `openai_client.py` — API Wrapper

Handles OpenAI API calls with:
- Retry logic with exponential backoff
- Token counting and logging
- Rate limit handling (429 responses)
- Streaming support for the `/api/rules/stream` endpoint

---

### `network/` — WebSocket Infrastructure

#### `websocket_client.py` — Resilient WS Client

A general-purpose WebSocket client with:

```python
class WebSocketClient:
    async def connect(max_retries=5, base_delay=2.0):
        # Exponential backoff with ±20% jitter
        # Special handling for 429 (rate limited) — starts with 10s base

    async def listen(callback):
        # Infinite reconnection loop
        # Resets retry count after 30s of stable connection
        # Calls callback(message) for each received message
```

#### `market_data_hub.py` — Connection Multiplexer

**Singleton pattern** — one WebSocket per upstream URL, shared by all subscribers:

```python
class MarketDataHub:
    _instances: Dict[str, MarketDataHub]  # Singleton per URL

    async def subscribe(callback):
        # Adds callback to subscriber set
        # If first subscriber: starts upstream connection
        # If not: reuses existing connection (multiplexing)

    async def unsubscribe(callback):
        # Removes callback
        # If last subscriber: closes upstream connection and purges instance

    async def _distribute(message):
        # Parses JSON once
        # Broadcasts to all subscribers via asyncio.gather()
```

**Why multiplexing?** If 5 sessions are monitoring the same symbol, we only need ONE upstream WebSocket connection to Alpaca, not five.

**Staggered start:** New hub connections are delayed by `random.uniform(0.1, 2.0)` seconds to prevent thundering herd on startup recovery.

---

### `broker/` — Brokerage Integration

#### `account_providers.py` — Alpaca Account Data

```python
class AlpacaAccountProvider:
    def get_snapshot(fields=None) -> dict:
        # Returns account data: buying_power, cash, etc.
        # Uses 5-second TTL cache to prevent REST spam
        # Fallback env vars: ALPACA_API_KEY → API_KEY → APCA_API_KEY_ID
```

#### `account_validation.py` — Pre-Flight Checks

```python
def validate_account_for_playbook(account, fields) -> List[str]:
    # Returns list of conflict messages (empty = good to go)
    # Checks: trading_blocked, trade_suspended, PDT limit, buying_power, cash
```

These checks run **before** session start to ensure the account is in a valid state for trading.

---

### `populate_tables.py` — Database Sync

After the LLM compiles a playbook, this module persists the structure to the Backend database:

```python
async def populate_playbook_tables(playbook_id, playbook):
    # For each RuleBlock in the Playbook:
    #   1. POST /rules/ → creates Rule record
    #   2. For each Extension:
    #      POST /conditions/ → creates Condition record
    #   3. Walk conditions tree:
    #      POST /condition-edges/ → creates ConditionEdge records
```

This creates the database representation that the Backend uses to display rules in the UI and for structural integrity validation during session start.

---

## API Reference

### Compilation Endpoints

| Method | Path | Purpose |
| :----- | :--- | :------ |
| `POST` | `/api/rules/compile` | Compile NL strategy into engine primitives |
| `GET` | `/api/rules/stream` | SSE stream of compilation progress |
| `POST` | `/api/rules/preview` | Stateless preview (no persistence) |

### Execution Endpoints

| Method | Path | Purpose |
| :----- | :--- | :------ |
| `POST` | `/api/rules/execute` | Start live evaluation for a session |
| `POST` | `/api/rules/stop` | Stop live evaluation |

### Analysis Endpoints

| Method | Path | Purpose |
| :----- | :--- | :------ |
| `POST` | `/api/rules/explain_deviation` | Generate AI explanation for a deviation |
| `POST` | `/api/rules/session_report_card` | Generate end-of-session audit report |

### Utility Endpoints

| Method | Path | Purpose |
| :----- | :--- | :------ |
| `GET` | `/health` | Health check |
| `GET` | `/active-tasks` | List running evaluation tasks |

---

## WebSocket Outputs

### Engine Output (`/ws/engine-output`)

Every evaluation tick broadcasts:

```json
{
  "type": "engine_state",
  "playbook_id": "uuid",
  "session_id": "uuid",
  "timestamp": "2026-05-05T14:30:00Z",
  "price": 97000.50,
  "rule_status": {
    "rule-id-1": true,
    "rule-id-2": false
  },
  "deviation": true,
  "deviation_true": ["Max Daily Loss Exceeded"],
  "deviation_false": ["Long Setup Valid", "Entry Conditions Met"],
  "action": false,
  "ai_reasoning": "The trader's cumulative daily loss has reached 3R...",
  "rule_evaluations": {
    "rule-id-2": {
      "ext_0": true,
      "ext_1": false
    }
  }
}
```

---

## Configuration

| Env Variable | Default | Purpose |
| :----------- | :------ | :------ |
| `API_KEY` | — | Alpaca API key |
| `SECRET_KEY` | — | Alpaca API secret |
| `OPENAI_KEY` | — | OpenAI API key for GPT-4 |
| `TMOM_BACKEND_BASE_URL` | `https://tmom-app-backend.onrender.com` | Backend API URL |
| `TMOM_BACKEND_WS_BASE_URL` | `ws://localhost:8000` | Backend WebSocket URL |

---

## Key Design Decisions

### 1. Why an asyncio.Queue for persistence?
Market ticks arrive at high frequency (10-100/s). Synchronous HTTP POSTs would create backpressure and slow down evaluation. The queue decouples evaluation from persistence, allowing the engine to drop events under extreme load rather than blocking.

### 2. Why MarketDataHub singleton?
Multiple sessions monitoring the same symbol should share one upstream WebSocket connection. The Hub pattern prevents redundant connections and reduces Alpaca rate limiting.

### 3. Why a repair loop for LLM parsing?
GPT-4 occasionally produces invalid JSON (missing commas, wrong types). Rather than failing immediately, the repair loop gives the LLM a chance to fix its own mistakes, dramatically improving compilation success rates.

### 4. Why sticky ACTION deviations?
ACTION deviations (user traded against rules) are ephemeral — they only happen at the moment of the trade. Without "stickiness", they would disappear from the UI on the next tick. The engine keeps them in `sticky_deviations` so they persist in the feed.

### 5. Why decouple AI reasoning from the main loop?
LLM calls take 2-5 seconds. If reasoning blocked the evaluation loop, the UI would freeze during high-volatility periods. Instead, the engine broadcasts a `"GENERATING..."` placeholder and backfills the reasoning asynchronously.

---

## Debugging Guide

### "Rules are not evaluating correctly"

1. Check the compiled playbook: `GET /api/rules/compile?playbook_id=...` or inspect `_compiled_playbooks` in memory.
2. Look at the `context` dict in the evaluation output — are the expected metrics present?
3. Check if the primitive is correctly registered in `PRIMITIVE_REGISTRY`.
4. For math expressions in `comparison_evaluator`, check the safe-eval log for parse failures.

### "Deviations are flickering in the UI"

1. Check `_last_deviation_set` in `EngineState` — is the comparison working?
2. Verify sticky deviation logic in `execution_engine.py`.
3. Check WebSocket connection stability (exponential backoff logs).

### "AI reasoning says GENERATING... forever"

1. Check OpenAI API key validity and rate limits.
2. Look for errors in `persistence_worker` logs.
3. Verify the PATCH call to update the session event is succeeding.

### "Session won't start"

1. Check Backend logs for validation failures (ownership, activation, structural integrity).
2. Verify the playbook has `generation_status=COMPLETED`.
3. Check if there's already an active session (only one per user allowed).

### "Auto-recovery not working"

1. Verify `TMOM_BACKEND_BASE_URL` is correct and reachable.
2. Check startup logs for "Auto-recovering session" messages.
3. Verify the session still has `status=STARTED` in the database.
