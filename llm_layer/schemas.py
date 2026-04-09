# schemas.py
from typing import List, Dict, Optional, Literal
from pydantic import BaseModel, Field, field_validator
import json
import os

# Load TA-Lib metadata
_current_dir = os.path.dirname(os.path.abspath(__file__))
_metadata_path = os.path.join(_current_dir, "talib_metadata.json")
with open(_metadata_path, 'r') as f:
    TALIB_METADATA = json.load(f)
VALID_TALIB_METRICS = list(TALIB_METADATA.keys())

RuleCategory = Literal["ENTRY", "PROCESS", "RISK", "DISCIPLINE", "EXIT", "OVERRIDES"]
Status = Literal["ok", "needs_clarification", "greeting", "unsupported"]

class TALibMetricSchema(BaseModel):
    name: str # e.g., 'RSI', 'EMA', 'ATR'
    timeperiod: Optional[int] = None
    params: Optional[Dict[str, float]] = None # For additional parameters like MACD fast/slow periods

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        v_upper = v.upper()
        if v_upper not in VALID_TALIB_METRICS:
            raise ValueError(f"'{v}' is not a valid TA-Lib function. Must be one of the supported TA-Lib metrics.")
        return v_upper


class ExtensionSchema(BaseModel):
    id: str
    primitive: str
    params: Dict[str, object]


from typing import List, Dict, Optional, Literal, Union

# ... (rest of imports)

class ConditionsSchema(BaseModel):
    all: Optional[List[Union[str, 'ConditionsSchema']]] = []
    any: Optional[List[Union[str, 'ConditionsSchema']]] = []
    none: Optional[List[Union[str, 'ConditionsSchema']]] = []

# This allows Pydantic to resolve the self-reference
ConditionsSchema.model_rebuild()

class RuleSkeletonSchema(BaseModel):
    name: str # e.g. "Long VWAP Setup", "Max Daily Loss Constraint"
    category: RuleCategory
    extensions: List[ExtensionSchema]
    conditions: Optional[ConditionsSchema] = None


class ContextSkeletonSchema(BaseModel):
    symbol: Optional[str] = None
    market_data: List[str] = []
    ta_lib_metrics: List[TALibMetricSchema] = Field(default_factory=list)
    account_fields: List[str] = []


class LLMResponseSchema(BaseModel):
    status: Status
    rules: List[RuleSkeletonSchema] = Field(default_factory=list)
    context_skeleton: Optional[ContextSkeletonSchema] = Field(default=None)
    reason: Optional[str] = None
    dialogue: Optional[str] = None

