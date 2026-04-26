from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Set

from engine import Extension, Playbook, Primitive, PrimitiveRegistry, RuleBlock, RuleCategory
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
    last_order_id: Optional[str] = None
    last_side: Optional[str] = None

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
                
                # Suffix Fallback: If metric is 'RSI_14', also allow it to be found via 'RSI' and vice-versa
                if "_" in flattened_key:
                    base_m = flattened_key.rsplit("_", 1)[0]
                    flattened.setdefault(base_m, metric_value)

    if "current_time" not in flattened and "timestamp" in flattened:
        flattened["current_time"] = flattened["timestamp"]

    return flattened


def build_session_history_context(session_events: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    history: Dict[str, list[str]] = {
        "trades": [],
        "side_flip": [],
        "stop_loss": [],
    }
    event_history: list[tuple[str, str]] = []
    last_fill_side: Optional[str] = None
    session_start_time: Optional[str] = None

    for event in session_events:
        event_type = str(event.get("type", "")).upper()
        event_data = event.get("event_data") or {}
        timestamp = event_data.get("timestamp") or event.get("timestamp")
        if not timestamp:
            continue

        if event_type == "TRADING":
            alpaca_event_type = str(event_data.get("alpaca_event_type", "")).lower()
            if alpaca_event_type == "fill":
                history["trades"].append(timestamp)

                side = event_data.get("side")
                if last_fill_side and side and side != last_fill_side:
                    history["side_flip"].append(timestamp)
                if side:
                    last_fill_side = side

                exit_reason = str(event_data.get("exit_reason", "")).lower()
                if exit_reason == "stop_loss" or event_data.get("stop_loss") is True:
                    history["stop_loss"].append(timestamp)

        if event_type == "SYSTEM":
            action = str(event_data.get("action", "")).lower()
            if action == "start_session":
                session_start_time = timestamp
            if action in {"loss", "win", "stop_loss"}:
                event_history.append((timestamp, action))

    return {
        "history": history,
        "event_history": event_history,
        "session_start_time": session_start_time,
    }


def _comparison_truth_means_violation(rule_name: str, extension: Extension) -> bool:
    params = extension.params
    left = str(params.get("left", "")).lower()
    right = params.get("right")
    right_str = str(right).lower()
    op = str(params.get("op", "")).strip()
    rule_name = rule_name.lower()

    if "stop" in rule_name or "stop" in left or "stop" in right_str:
        return True
    if "loss" in rule_name or "loss" in left or "drawdown" in rule_name:
        return True
    if "position size" in rule_name or "max one position" in rule_name:
        return False
    if op in {"<=", ">=", "=="}:
        return False
    if isinstance(right, (int, float)) and float(right) < 0 and op in {"<", "<="}:
        return True

    return False


def _constraint_rule_is_deviation(rule: RuleBlock, rule_is_true: bool) -> bool:
    if not rule.extensions:
        return rule_is_true

    # Constraint-style rules often encode the *allowed* state directly.
    # We infer whether a truthy result means "compliant" or "violating"
    # from the primitives and parameter shapes currently produced by the parser.
    truth_means_violation = False

    for extension in rule.extensions.values():
        primitive_name = extension.primitive_name
        if primitive_name in {"rate_limit", "account_comparison", "set_membership"}:
            truth_means_violation = False
            break
        if primitive_name in {"accumulation", "sequence"}:
            truth_means_violation = True
            break
        if primitive_name == "comparison":
            truth_means_violation = _comparison_truth_means_violation(rule.name, extension)
            break

    return rule_is_true if truth_means_violation else not rule_is_true


def _is_action_gated_constraint(rule: RuleBlock) -> bool:
    """
    Determines if a constraint restricts ACTIONS (thus requiring user_action_bool to deviate)
    or restricts STATE (triggering continuous deviation if held).
    """
    cat_name = rule.category.name if hasattr(rule.category, "name") else str(rule.category)
    if cat_name in {"ENTRY", "EXIT", "PROCESS"}:
        return True

    if not rule.extensions:
        return True

    for extension in rule.extensions.values():
        primitive_name = extension.primitive_name
        rule_name = rule.name.lower()

        if primitive_name in {"temporal_gate", "rate_limit", "account_comparison", "set_membership"}:
            return True

        if primitive_name in {"accumulation", "sequence"}:
            return True

        if primitive_name == "comparison":
            if "stop" in rule_name or "target" in rule_name:
                return False
            if "daily" in rule_name or "drawdown" in rule_name:
                return True

    return True


def _evaluate_rule_permission(rule: RuleBlock, rule_is_true: bool) -> bool:
    """
    Returns True if the current state PERMITS the action associated with this rule.
    (Green Check = True, Red X = False)
    """
    # For Entry behaviors, 'True' means the signal is active and you are allowed to enter.
    if rule.category in {RuleCategory.ENTRY, RuleCategory.PROCESS}:
        return rule_is_true
        
    # For Constraints (Risk, Discipline, etc), 'is_deviation' means the rule is broken.
    # Therefore, permission is the inverse of the deviation state.
    return not _constraint_rule_is_deviation(rule, rule_is_true)


def _classify_rule_deviation(rule: RuleBlock, rule_is_true: bool, user_action_bool: bool) -> bool:
    cat_name = rule.category.name if hasattr(rule.category, "name") else str(rule.category)
    
    # ENTRY / PROCESS rules describe the action the trader should have taken.
    if cat_name in {"ENTRY", "PROCESS"}:
        return user_action_bool and not rule_is_true

    # RISK / DISCIPLINE / EXIT / OVERRIDES act like active violations or constraints.
    if cat_name in {"RISK", "DISCIPLINE", "EXIT", "OVERRIDES"}:
        is_violating = _constraint_rule_is_deviation(rule, rule_is_true)
        if _is_action_gated_constraint(rule):
            return user_action_bool and is_violating
        return is_violating

    return False


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
    triggered_entry_rules = [
        rule
        for rule in playbook.rules
        if rule.category == RuleCategory.ENTRY and (rule.id or rule.name) in entry_triggers
    ]

    triggered_entry_ids = [
        str(rule.id) if rule.id else rule.name
        for rule in triggered_entry_rules
    ]
    if not triggered_entry_ids and entry_triggers:
        triggered_entry_ids = [str(trigger) for trigger in entry_triggers]

    inferred_side = None
    for rule in triggered_entry_rules:
        rule_name = rule.name.lower()
        if "short" in rule_name or "sell" in rule_name:
            inferred_side = "sell"
            break
        if "long" in rule_name or "buy" in rule_name:
            inferred_side = "buy"
            break

    rule_evaluations: Dict[str, bool] = {}
    rule_status: Dict[str, bool] = {}
    deviation_true = []
    deviation_false = []

    for rule in playbook.rules:
        rule_identifier = str(rule.id) if rule.id else rule.name
        result_keys = playbook_results.get(rule.category, [])
        rule_is_true = (rule.id or rule.name) in result_keys
        rule_evaluations[rule_identifier] = rule_is_true

        is_deviation = _classify_rule_deviation(rule, rule_is_true, user_action_bool)
        if is_deviation:
            deviation_true.append(rule_identifier)
        else:
            deviation_false.append(rule_identifier)
        
        # 4. Icon Status (GO/NO-GO)
        # This is independent of the user's action and represents the "current gate state" for the UI.
        rule_status[rule_identifier] = _evaluate_rule_permission(rule, rule_is_true)

    overall_deviation = bool(deviation_true)
    accumulated_deviation = state.record_deviation(overall_deviation)
    rule_summary = ", ".join(triggered_entry_ids) if triggered_entry_ids else "No entry rule triggered"

    return {
        "timestamp": context.get("current_time") or context.get("timestamp"),
        "price": context.get("price"),
        "symbol": context.get("symbol"),
        "side": state.last_side or inferred_side or "buy",
        "playbook_id": playbook_id,
        "session_id": session_id,
        "user_id": user_id,
        "rule": rule_summary,
        "rule_triggered": len(triggered_entry_ids) > 0,
        "triggered_entries": triggered_entry_ids,
        "rule_evaluations": rule_evaluations,
        "rule_status": rule_status,
        "action": user_action_bool,
        "order_id": getattr(state, "last_order_id", None),
        "deviation": overall_deviation,
        "deviation_true": deviation_true,
        "deviation_false": deviation_false,
        "accumulated_deviation": accumulated_deviation,
    }

