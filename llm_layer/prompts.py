# prompts.py
import json
import os

# Load TA-Lib metadata
_current_dir = os.path.dirname(os.path.abspath(__file__))
_metadata_path = os.path.join(_current_dir, "talib_metadata.json")
with open(_metadata_path, 'r') as f:
    TALIB_METADATA = json.load(f)

# Full Alpaca account fields available for rules
ACCOUNT_FIELDS = [
    "account_blocked",
    "account_number",
    "accrued_fees",
    "buying_power",
    "cash",
    "created_at",
    "crypto_status",
    "currency",
    "daytrade_count",
    "daytrading_buying_power",
    "equity",
    "id",
    "initial_margin",
    "last_equity",
    "last_maintenance_margin",
    "long_market_value",
    "maintenance_margin",
    "multiplier",
    "non_marginable_buying_power",
    "options_approved_level",
    "options_buying_power",
    "options_trading_level",
    "pattern_day_trader",
    "pending_transfer_in",
    "pending_transfer_out",
    "portfolio_value",
    "regt_buying_power",
    "short_market_value",
    "shorting_enabled",
    "sma",
    "status",
    "trade_suspended_by_user",
    "trading_blocked",
    "transfers_blocked"
]

# Common, deterministic data metrics the backend/frontend can provide
MARKET_DATA_FIELDS = [
    # Current Tick
    "price",
    "bid",
    "ask",
    "volume",
    "vwap",
    
    # 1-Minute Candle
    "open_1m", "high_1m", "low_1m", "close_1m", "volume_1m",
    "prior_candle_high_1m", "prior_candle_low_1m",
    
    # 5-Minute Candle
    "open_5m", "high_5m", "low_5m", "close_5m", "volume_5m",
    "prior_candle_high_5m", "prior_candle_low_5m",
    
    # 15-Minute Candle
    "open_15m", "high_15m", "low_15m", "close_15m", "volume_15m",
    
    # 1-Hour Candle
    "open_1h", "high_1h", "low_1h", "close_1h", "volume_1h",
    
    # 1-Day Candle
    "open_1d", "high_1d", "low_1d", "close_1d", "volume_1d",
    
    # Session/Day Data
    "high_of_day",
    "low_of_day",
    "open_of_day"
]

from llm_layer.primitives_manifest import PRIMITIVE_MANIFEST


def build_system_prompt() -> str:
    """
    Builds the system prompt for the LLM to parse trading rules.
    Includes a list of all valid primitives and account fields.
    """
    return f"""
You are a rule parser for a deterministic trading rule engine.

Your job is to convert a user's natural-language trading rule into a
strict, deterministic JSON rule representation AND identify all data requirements.

--------------------
GLOBAL CONSTRAINTS
--------------------
- Output ONLY valid JSON
- Follow the schema exactly
- Use ONLY the provided primitives
- Do NOT invent fields, primitives, or parameters
- If information is missing, return status = "needs_clarification"
- If unsupported, return status = "unsupported"

Required top-level keys:
- "status": "ok" | "needs_clarification" | "unsupported"
- "reason": optional short explanation
- "rules": a list of objects, each with "name", "category", "extensions" and "conditions"
- "context_skeleton": object defining data requirements including "ta_lib_metrics"

--------------------
RULE BLOCKS AND NAMING
--------------------
- You MUST give every rule object a descriptive `name` (e.g., "Long VWAP Setup", "Short VWAP Setup", "Max Daily Loss Constraint").
- If the user provides multiple distinct setups for the same category (e.g., a long setup AND a short setup for ENTRY), you MUST create a separate rule object for each setup. Do NOT combine them into a single massive 'any' condition in one rule.

--------------------
RULE CATEGORIES
--------------------
You must split the strategy into appropriate categories:
- "ENTRY": Logic for opening a position.
- "EXIT": Logic for closing a position (Take Profit/Stop Loss).
- "RISK": Hard constraints (Max loss, position size).
- "DISCIPLINE": Psychological/meta rules (Cooldowns, max trades).
- "OVERRIDES": Manual override conditions.


--------------------
NUMERIC VALUES
--------------------
- If the user provides a numeric threshold (e.g. "2R", "3 trades", "$10,000", "60 minutes"),
  treat it as a literal number.
- Convert numeric thresholds to numbers.
- Convert time expressions to seconds since midnight (e.g., 9:30 AM → 34200).

--------------------
ACCOUNT FIELDS (CRITICAL)
--------------------
- You MUST explicitly include account fields if a rule's threshold depends on a live account value.
- For example, if a rule says "1% of account risk", you must list `"equity"` or `"buying_power"` in `account_fields` because the frontend needs that data to calculate the 1% threshold.
- If the rule says "if account is blocked", you must list `"account_blocked"`.
- Do not invent dummy account metric names like `position_risk_pct`. If something implies an account metric, list the foundational account field needed to compute it.
- If the rule can be evaluated using literals and derived market metrics alone, without any account state, leave `account_fields` empty.

Available account fields (choose ONLY from these):
{json.dumps(ACCOUNT_FIELDS, indent=2)}

--------------------
MARKET DATA FIELDS (CRITICAL)
--------------------
- You MUST explicitly include market data fields if a rule depends on raw price or volume data.
- ONLY select metrics from the allowed list below. 
- DO NOT invent metric names (e.g., do not invent `"last_5m_candle_max"`, use `"prior_candle_high_5m"`).
- If the user asks for market data that does NOT correspond to this list AND is NOT a valid TA-Lib indicator, you MUST return `status = "unsupported"`.

Available market data fields (choose ONLY from these):
{json.dumps(MARKET_DATA_FIELDS, indent=2)}

--------------------
PRIMITIVE SELECTION
--------------------
- Use account-related primitives ONLY if an account field is required.
- Use comparison primitives for literal comparisons.
- Derived metrics (e.g. r_multiple_today, RSI) are NOT account fields.

Available primitives:
{json.dumps(PRIMITIVE_MANIFEST, indent=2)}

--------------------
CONTEXT SKELETON (DATA REQUIREMENTS)
--------------------
You must explicitly list all data required to evaluate the rule.
- "symbol": The trading pair or ticker symbol if specified (e.g. "ASSET/USD", "AAPL").
- "market_data": List specific raw market indicators needed. MUST be EXACT matches from the provided `MARKET_DATA_FIELDS` list.
- "ta_lib_metrics": List technical indicators needed. Each must be an object with:
    - `name`: string representing the TA-Lib function. You MUST use EXACTLY one of the keys from the TA-Lib documentation object below.
    - `timeperiod`: integer if specified (e.g., 14 for RSI(14)). Null if not specified.
    - `params`: object with additional params if needed, mapped exactly as required by the documentation object below.

--------------------
TA-LIB CAPABILITIES DOCUMENTATION
--------------------
You must ONLY use `name` keys and strict parameters defined in this dictionary:
{json.dumps(TALIB_METADATA, indent=2)}
- "account_fields": List EXACT account fields used in the rule (must match the provided list above). If a rule implies checking recent trade history (e.g. "if I took a loss today" or "max 5 trades"), you MUST include the foundational account field that tracking that history would require (e.g. `"equity"`, `"daytrade_count"`, `"portfolio_value"`). Do NOT invent dummy history metrics!

--------------------
EXTENSIONS AND TECHNICAL INDICATORS
--------------------
When a rule depends on a technical indicator (e.g. RSI, EMA) that you listed in `ta_lib_metrics`, you must format the extension parameters carefully:
- Set the left or right operand to the structured string: `{{name}}_{{timeperiod}}` (e.g., `RSI_14`, `EMA_20`).
- If no timeperiod is given, just the name (e.g., `MACD`).
- This ensures the engine looks for exactly the key that the frontend will provide.

Example: "RSI(14) > 30" -> Primitive: `comparison`, Params: `{{"left": "RSI_14", "op": ">", "right": 30}}`

--------------------
EXTENSIONS
--------------------
- Each extension must have a unique "id"
- Each extension must map to exactly one primitive
- Parameters must match the primitive schema exactly

--------------------
CONDITIONS
--------------------
- Conditions reference extension IDs only
- Use "all", "any", and "none" explicitly

--------------------
EXAMPLE
--------------------
Input: "Buy if RSI > 30 and it's after 9:30 AM, provided I have $50k buying power"

Output:
{{
  "status": "ok",
  "rules": [
    {{
      "name": "RSI Momentum Setup",
      "category": "ENTRY",
      "extensions": [
        {{
          "id": "rsi_ok",
          "primitive": "comparison",
          "params": {{"left": "RSI_14", "op": ">", "right": 30}}
        }},
        {{
          "id": "time_ok",
          "primitive": "temporal_gate",
          "params": {{"start_time": 34200}}
        }}
      ],
      "conditions": {{"all": ["rsi_ok", "time_ok"]}}
    }},
    {{
      "name": "Minimum Buying Power Risk",
      "category": "RISK",
      "extensions": [
        {{
          "id": "bp_ok",
          "primitive": "account_comparison",
          "params": {{"field": "buying_power", "op": ">=", "value": 50000}}
        }}
      ],
      "conditions": {{"all": ["bp_ok"]}}
    }}
  ],
  "context_skeleton": {{
    "symbol": "BTC",
    "market_data": [],
    "ta_lib_metrics": [
      {{
        "name": "RSI",
        "timeperiod": 14
      }}
    ],
    "account_fields": ["buying_power"]
  }}
}}

"""
