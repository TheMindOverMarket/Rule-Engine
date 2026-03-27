from dataclasses import dataclass
from typing import Any, Dict, Optional, Set

from engine import Playbook, Primitive, PrimitiveRegistry, RuleCategory
from primitives import (
    accumulation_evaluator,
    account_comparison_evaluator,
    comparison_evaluator,
    rate_limit_evaluator,
    sequence_evaluator,
    set_membership_evaluator,
    temporal_gate_evaluator,
)


ENTRY_LIKE_CATEGORIES: Set[RuleCategory] = {
    RuleCategory.ENTRY,
    RuleCategory.PROCESS,
}
CONSTRAINT_CATEGORIES: Set[RuleCategory] = {
    RuleCategory.RISK,
    RuleCategory.DISCIPLINE,
    RuleCategory.EXIT,
    RuleCategory.OVERRIDES,
}


def register_default_primitives() -> None:
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


@dataclass
class EngineState:
    user_took_action: bool = False
    accumulated_deviation: int = 0

    def consume_user_action(self) -> bool:
        acted = self.user_took_action
        self.user_took_action = False
        return acted

    async def get_and_reset_user_action(self) -> bool:
        return self.consume_user_action()

    def record_deviation(self, deviation: bool) -> int:
        if deviation:
            self.accumulated_deviation += 1
        return self.accumulated_deviation


def flatten_market_payload(data: Dict[str, Any]) -> Dict[str, Any]:
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


def _classify_rule_deviation(rule_category: RuleCategory, rule_is_true: bool, user_action_bool: bool) -> bool:
    # ENTRY / PROCESS rules describe the action the trader should have taken.
    if rule_category in ENTRY_LIKE_CATEGORIES:
        return rule_is_true != user_action_bool

    # RISK / DISCIPLINE / EXIT / OVERRIDES act like active violations or constraints.
    if rule_category in CONSTRAINT_CATEGORIES:
        return rule_is_true

    return rule_is_true != user_action_bool


def build_logic_adherence_payload(
    playbook: Playbook,
    context: Dict[str, Any],
    user_action_bool: bool,
    state: EngineState,
    playbook_id: str,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    playbook_results = playbook.evaluate(context)
    entry_triggers = playbook_results.get(RuleCategory.ENTRY, [])

    triggered_entry_ids = [
        str(rule.id) if rule.id else rule.name
        for rule in playbook.rules
        if rule.category == RuleCategory.ENTRY and (rule.id or rule.name) in entry_triggers
    ]
    if not triggered_entry_ids and entry_triggers:
        triggered_entry_ids = [str(trigger) for trigger in entry_triggers]

    rule_evaluations: Dict[str, bool] = {}
    deviation_true = []
    deviation_false = []

    for rule in playbook.rules:
        rule_identifier = str(rule.id) if rule.id else rule.name
        result_keys = playbook_results.get(rule.category, [])
        rule_is_true = (rule.id or rule.name) in result_keys
        rule_evaluations[rule_identifier] = rule_is_true

        is_deviation = _classify_rule_deviation(rule.category, rule_is_true, user_action_bool)
        if is_deviation:
            deviation_true.append(rule_identifier)
        else:
            deviation_false.append(rule_identifier)

    overall_deviation = bool(deviation_true)
    accumulated_deviation = state.record_deviation(overall_deviation)
    rule_summary = ", ".join(triggered_entry_ids) if triggered_entry_ids else "No entry rule triggered"

    return {
        "timestamp": context.get("current_time") or context.get("timestamp"),
        "price": context.get("price"),
        "playbook_id": playbook_id,
        "session_id": session_id,
        "user_id": user_id,
        "rule": rule_summary,
        "rule_triggered": len(triggered_entry_ids) > 0,
        "triggered_entries": triggered_entry_ids,
        "rule_evaluations": rule_evaluations,
        "action": user_action_bool,
        "deviation": overall_deviation,
        "deviation_true": deviation_true,
        "deviation_false": deviation_false,
        "accumulated_deviation": accumulated_deviation,
    }
