import json
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from engine import Playbook, RuleBlock, RuleCategory
from logic_adherence import EngineState, build_logic_adherence_payload, register_default_primitives


FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"


def load_fixture(name: str):
    return json.loads((FIXTURES_DIR / name).read_text())


def build_playbook_from_fixture(name: str) -> Playbook:
    fixture = load_fixture(name)
    playbook = Playbook(name=fixture.get("name", "Fixture Playbook"))

    for rule_data in fixture["rules"]:
        category = RuleCategory[rule_data["category"]]
        rule = RuleBlock(category=category, skeleton=rule_data)
        rule.id = rule_data.get("id")
        playbook.add_rule(rule)

    return playbook


class LogicAdherenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        register_default_primitives()

    def setUp(self) -> None:
        self.playbook = build_playbook_from_fixture("playbook_long_setup.json")
        self.state = EngineState()

    def test_long_setup_is_green_when_rules_are_met_and_user_acts(self) -> None:
        market_state = load_fixture("market_state_long_setup_ready.json")

        payload = build_logic_adherence_payload(
            playbook=self.playbook,
            context=market_state,
            user_action_bool=True,
            state=self.state,
            playbook_id="demo-playbook",
        )

        self.assertFalse(payload["deviation"])
        self.assertEqual(payload["deviation_true"], [])
        self.assertIn("long_setup", payload["deviation_false"])
        self.assertIn("stop_hit", payload["deviation_false"])
        self.assertIn("daily_loss_limit_hit", payload["deviation_false"])
        self.assertEqual(payload["triggered_entries"], ["long_setup"])
        self.assertEqual(payload["accumulated_deviation"], 0)

    def test_stop_hit_is_flagged_red(self) -> None:
        market_state = load_fixture("market_state_stop_hit.json")

        payload = build_logic_adherence_payload(
            playbook=self.playbook,
            context=market_state,
            user_action_bool=True,
            state=self.state,
            playbook_id="demo-playbook",
        )

        self.assertTrue(payload["deviation"])
        self.assertIn("stop_hit", payload["deviation_true"])
        self.assertEqual(payload["accumulated_deviation"], 1)

    def test_hard_constraint_violation_is_flagged_red(self) -> None:
        market_state = load_fixture("market_state_hard_constraint.json")

        payload = build_logic_adherence_payload(
            playbook=self.playbook,
            context=market_state,
            user_action_bool=True,
            state=self.state,
            playbook_id="demo-playbook",
        )

        self.assertTrue(payload["deviation"])
        self.assertIn("daily_loss_limit_hit", payload["deviation_true"])
        self.assertEqual(payload["accumulated_deviation"], 1)

    def test_accumulated_deviation_counts_across_sequential_events(self) -> None:
        scenarios = [
            ("market_state_long_setup_ready.json", True, 0),
            ("market_state_stop_hit.json", True, 1),
            ("market_state_hard_constraint.json", True, 2),
        ]

        last_payload = None
        for fixture_name, user_action, expected_total in scenarios:
            last_payload = build_logic_adherence_payload(
                playbook=self.playbook,
                context=load_fixture(fixture_name),
                user_action_bool=user_action,
                state=self.state,
                playbook_id="demo-playbook",
            )
            self.assertEqual(last_payload["accumulated_deviation"], expected_total)

        self.assertIsNotNone(last_payload)
        self.assertTrue(last_payload["deviation"])
        self.assertIn("daily_loss_limit_hit", last_payload["deviation_true"])


if __name__ == "__main__":
    unittest.main()
