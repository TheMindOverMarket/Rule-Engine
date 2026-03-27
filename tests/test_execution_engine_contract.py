import ast
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXECUTION_ENGINE_PATH = PROJECT_ROOT / "execution_engine.py"


class ExecutionEngineContractTests(unittest.TestCase):
    def test_execution_engine_uses_exported_flatten_market_payload_name(self) -> None:
        tree = ast.parse(EXECUTION_ENGINE_PATH.read_text(), filename=str(EXECUTION_ENGINE_PATH))

        loaded_names = {
            node.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
        }

        self.assertIn("flatten_market_payload", loaded_names)
        self.assertNotIn("_flatten_market_payload", loaded_names)


if __name__ == "__main__":
    unittest.main()
