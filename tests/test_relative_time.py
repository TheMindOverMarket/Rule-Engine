
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

if __name__ == "__main__":
    unittest.main()
