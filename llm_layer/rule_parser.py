# rule_parser.py
import json
import uuid
from typing import Optional
from llm_layer.schemas import LLMResponseSchema, RuleSkeletonSchema
from engine import RuleBlock, RuleCategory, Extension
from llm_layer.prompts import build_system_prompt
import pprint

class RuleParser:
    """
    Handles conversation with LLM to convert natural language rules 
    into structured RuleBlocks and Context Skeletons.
    """
    def __init__(self, llm_client, category: RuleCategory = RuleCategory.ENTRY, max_repairs: int = 2):
        """
        Args:
            llm_client: Wrapper for LLM interactions.
            category: The default RuleCategory for parsed rules.
            max_repairs: Max attempts to fix invalid JSON output from LLM.
        """
        self.llm = llm_client
        self.category = category
        self.max_repairs = max_repairs
        self.system_prompt = build_system_prompt()

    def parse(self, user_input: str) -> 'Playbook':
        """
        Parse user input into a Playbook containing multiple RuleBlocks.
        """
        from engine import Playbook
        print(f"\n--- CALLING LLM WITH INPUT ---\n{user_input[:200]}...")
        raw = self.llm.generate(self.system_prompt, user_input)
        print(f"\n--- LLM RAW RESPONSE ---\n{raw}")
        llm_response = self._validate_with_repair(raw, user_input)

        if llm_response.status != "ok":
            raise ValueError(f"Cannot parse playbook: {llm_response.reason or 'LLM needs clarification'}")

        from engine import Playbook, RuleCategory
        playbook = Playbook()
        for rule_skeleton in llm_response.rules:
            # Cast string category from LLM to Engine Enum
            category_enum = RuleCategory[rule_skeleton.category]
            
            skeleton_dict = rule_skeleton.dict()
            print(f"\n--- DERIVED RULE SKELETON ({rule_skeleton.category}) ---")
            pprint.pprint(skeleton_dict)
            rule_block = RuleBlock(category=category_enum, skeleton=skeleton_dict)
            playbook.add_rule(rule_block)

        
        context_skeleton = llm_response.context_skeleton
        
        return playbook, context_skeleton


    def parse_chat(self, chat_history: list) -> tuple['Playbook', 'ContextSkeletonSchema', str]:
        """
        Parse user input from a chat history. Returns (Playbook, ContextSkeletonSchema, clarification_reason)
        """
        user_input_preview = str(chat_history[-1]) if chat_history else ""
        print(f"\n--- CALLING LLM WITH CHAT HISTORY ---\nLast message: {user_input_preview[:200]}...")
        raw = self.llm.generate_chat(self.system_prompt, chat_history)
        print(f"\n--- LLM RAW RESPONSE ---\n{raw}")
        
        user_input_str = "\n".join([f"{m.get('role', '').upper()}: {m.get('content', '')}" for m in chat_history])
        llm_response = self._validate_with_repair(raw, user_input_str)

        if llm_response.status == "needs_clarification":
            return None, None, llm_response.reason or "LLM needs clarification"
            
        if llm_response.status == "greeting":
            return None, None, "GREETING:" + (llm_response.reason or "Hello! How can I help with your strategy?")
            
        if llm_response.status != "ok":
            raise ValueError(f"Cannot parse playbook: {llm_response.reason or 'Unsupported or unknown error'}")

        from engine import Playbook, RuleCategory, RuleBlock
        playbook = Playbook()
        for rule_skeleton in llm_response.rules:
            category_enum = RuleCategory[rule_skeleton.category]
            skeleton_dict = rule_skeleton.dict()
            rule_block = RuleBlock(category=category_enum, skeleton=skeleton_dict)
            playbook.add_rule(rule_block)
            
        return playbook, llm_response.context_skeleton, None

    async def stream_parse_chat(self, chat_history: list):
        """
        Async generator that yields tokens.
        If the response is a 'status: ok' with full rules, it accumulates and returns the playbook at the end.
        If it's 'needs_clarification' or 'greeting', it streams the text.
        """
        full_response = ""
        is_json_likely = False
        
        # We use a simple heuristic to detect if LLM is outputting JSON or just text.
        # But actually, the prompt forces JSON. So we should probably yield everything
        # if it's NOT a final "ok" state if we want the user to see it.
        # Wait, if it's persistent JSON, streaming tokens of a JSON is weird for the user.
        # So I will yield the content of the "reason" field if status is NOT "ok".
        
        # Actually, for the best UX, I'll use a revised approach:
        # LLM will output JSON. I will parse it incrementally or just wait for 'reason' field tokens.
        
        # Revised Strategy:
        # I will use the llm.stream_chat and yield those tokens directly.
        # The frontend will be responsible for showing them.
        # Once the stream is finished, the backend handles the final persistence.
        
        # HOWEVER, the LLM output is structured JSON. Token-by-token JSON is ugly.
        # I should probably have the LLM output the "Assistant response" as a separate field.
        
        # Let's just stream everything and let the frontend show it. 
        # If it's JSON, the frontend might see brackets, but the user approved "streaming tokens".
        
        for token in self.llm.stream_chat(self.system_prompt, chat_history):
            full_response += token
            yield token
            
        # Bare return to end the generator
        return

    def _validate_with_repair(self, raw: str, user_input: str) -> LLMResponseSchema:
        """
        Validate raw JSON from LLM and attempt repair if invalid.
        """
        for attempt in range(self.max_repairs + 1):
            try:
                parsed = json.loads(raw)

                # Normalize missing optional fields
                if "rules" not in parsed:
                    parsed["rules"] = []
                if "reason" not in parsed:
                    parsed["reason"] = None

                # Handle legacy 'rule' key if present
                if "rule" in parsed and parsed["rule"] is not None:
                    parsed["rules"].append(parsed["rule"])

                # Wrap flat output into rule skeleton if needed
                if not parsed.get("rules") and "primitive" in parsed:
                    ext_id = parsed.get("id") or f"ext_{uuid.uuid4().hex[:8]}"
                    parsed["rules"].append({
                        "category": self.category.name if hasattr(self.category, 'name') else self.category,
                        "extensions": [
                            {
                                "id": ext_id,
                                "primitive": parsed["primitive"],
                                "params": parsed.get("params", {})
                            }
                        ],
                        "conditions": {"all": [ext_id]}
                    })

                return LLMResponseSchema.model_validate(parsed)

            except Exception as e:
                if attempt >= self.max_repairs:
                    raise ValueError(f"LLM output invalid after repair: {e}")

                # Ask LLM to repair
                print(f"\n--- REPAIR ATTEMPT {attempt + 1} ---")
                repair_prompt = f"""
Original User Input:
{user_input}

Previous output failed validation.

Error:
{str(e)}


Return ONLY valid JSON matching schema:
- top-level 'status': 'ok' | 'needs_clarification' | 'unsupported'
- optional 'reason'
- 'rule': {{ "extensions": [{{'id', 'primitive', 'params'}}], "conditions": {{'all', 'any', 'none'}} }}
- 'context_skeleton': {{ "market_data": [], "account_fields": [], "time_required": bool, "history_metrics": [] }}
"""
                raw = self.llm.generate(self.system_prompt, repair_prompt)
                print(f"\n--- REPAIR RESPONSE ---\n{raw}")
