# primitives_manifest.py
from llm_layer.prompts import ACCOUNT_FIELDS

PRIMITIVE_MANIFEST = {
    "comparison": {
        "description": "Compare a market or derived context value against a constant",
        "context": "market",
        "params": {
            "left": "string (market or derived field name)",
            "op": [">", ">=", "<", "<=", "=="],
            "right": "number"
        },
        "example": {"left": "rsi", "op": ">", "right": 30}
    },

    "set_membership": {
        "description": "Restrict a context field to allowed or forbidden sets",
        "context": "market",
        "params": {
            "field": "string (context field name)",
            "allowed": ["string"],
            "forbidden": ["string"]
        },
        "example": {"field": "market_regime", "allowed": ["trend", "range"]}
    },

    "rate_limit": {
        "description": "Limit how often an event occurs within a rolling time window",
        "context": "event_history",
        "params": {
            "metric": "string (event name)",
            "max": "number",
            "window_minutes": "number"
        },
        "example": {"metric": "trades", "max": 5, "window_minutes": 60}
    },

    "accumulation": {
        "description": "Evaluate an accumulated metric against a threshold",
        "context": "derived",
        "params": {
            "field": "string (accumulated metric name)",
            "threshold": "number",
            "op": [">", ">=", "<", "<=", "=="]
        },
        "example": {"field": "select from this list: " + ", ".join(ACCOUNT_FIELDS), "threshold": 1000, "op": ">"}
    },

    "sequence": {
        "description": "Detect an ordered sequence of events within an optional time window",
        "context": "event_history",
        "params": {
            "pattern": ["string"],
            "window_minutes": "number | null"
        },
        "example": {"pattern": ["loss", "loss", "loss"], "window_minutes": 30}
    },

    "temporal_gate": {
        "description": "Restrict rule execution based on time-of-day or cooldown windows",
        "context": "time",
        "params": {
            "start_time": "number | null (seconds since midnight)",
            "end_time": "number | null (seconds since midnight)",
            "cooldown_end": "number | null (unix timestamp)",
            "start_offset_minutes": "number | null (minutes since session start)",
            "end_offset_minutes": "number | null (minutes since session start)"
        },
        "example": {"start_time": 34200, "end_time": 41400}
    },

    "account_comparison": {
        "description": "Compare a broker account field against a numeric threshold",
        "context": "account",
        "params": {
            "field": "string (any valid account field from the provided list: " + ", ".join(ACCOUNT_FIELDS) + ")",
            "op": [">", ">=", "<", "<=", "=="],
            "value": "number"
        },
        "example": {"field": "<account_field>", "op": ">=", "value": 50000}
    }
}
