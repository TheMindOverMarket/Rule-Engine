
import unittest
from datetime import datetime, timezone, timedelta
from logic_adherence import build_session_history_context
from primitives import temporal_gate_evaluator

class TestRelativeTime(unittest.TestCase):
    def test_build_session_history_context_start_time(self):
        events = [
            {
                "type": "SYSTEM",
                "event_data": {"action": "START_SESSION", "status": "started"},
                "timestamp": "2026-04-07T10:00:00Z"
            },
            {
                "type": "TRADING",
                "event_data": {"alpaca_event_type": "fill", "side": "buy", "timestamp": "2026-04-07T10:01:00Z"},
            }
        ]
        context = build_session_history_context(events)
        self.assertEqual(context["session_start_time"], "2026-04-07T10:00:00Z")
        self.assertEqual(len(context["history"]["trades"]), 1)

    def test_temporal_gate_offset(self):
        # Case 1: 5 minutes after start, gate 5 min
        context = {
            "minutes_since_start": 5.0,
            "current_time": "2026-04-07T10:05:00Z"
        }
        params = {"start_offset_minutes": 5}
        self.assertTrue(temporal_gate_evaluator(params, context))

        # Case 2: 4 minutes after start, gate 5 min
        context["minutes_since_start"] = 4.9
        self.assertFalse(temporal_gate_evaluator(params, context))

        # Case 3: End offset
        params = {"end_offset_minutes": 10}
        context["minutes_since_start"] = 11.0
        self.assertFalse(temporal_gate_evaluator(params, context))
        
        context["minutes_since_start"] = 9.0
        self.assertTrue(temporal_gate_evaluator(params, context))

    def test_combined_absolute_relative(self):
        # 10:05 AM is 36300 seconds since midnight
        context = {
            "minutes_since_start": 5.0,
            "current_time": "2026-04-07T10:05:00Z"
        }
        # Start at 10:00 AM (36000) AND 10 mins after session start
        params = {
            "start_time": 36000, 
            "start_offset_minutes": 10
        }
        self.assertFalse(temporal_gate_evaluator(params, context))
        
        context["minutes_since_start"] = 11.0
        self.assertTrue(temporal_gate_evaluator(params, context))

    def test_evaluate_rule_permission(self):
        from engine import RuleBlock, RuleCategory, PrimitiveRegistry, Primitive
        from logic_adherence import _evaluate_rule_permission

        # Ensure primitives are registered
        try:
             PrimitiveRegistry.register(Primitive("comparison", lambda p, c: True))
        except:
             pass

        # ENTRY Rule: Permission = rule_is_true
        entry_rule = RuleBlock(RuleCategory.ENTRY, {"name": "Entry Rule", "extensions": []})
        self.assertTrue(_evaluate_rule_permission(entry_rule, True))
        self.assertFalse(_evaluate_rule_permission(entry_rule, False))

        # RISK Rule (Constraint): Permission = not deviation
        risk_rule = RuleBlock(RuleCategory.RISK, {"name": "Max Drawdown", "extensions": [{"id": "ext1", "primitive": "comparison", "params": {"left": "equity", "op": ">", "right": 50000}}]})
        # If rule is 'True' meaning we are above 50k, we are compliant.
        # But wait, logic_adherence uses _constraint_rule_is_deviation. 
        # For 'comparison', truth_means_violation depends on keywords. 
        # 'drawdown' in name means True=Violation.
        self.assertFalse(_evaluate_rule_permission(risk_rule, True)) # True = Violation = False Permission
        self.assertTrue(_evaluate_rule_permission(risk_rule, False)) # False = Compliant = True Permission

if __name__ == "__main__":
    unittest.main()
