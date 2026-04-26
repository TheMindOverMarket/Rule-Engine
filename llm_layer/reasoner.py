from llm_layer.openai_client import OpenAILLMClient
import json

class DeviationReasoner:
    def __init__(self, llm_client: OpenAILLMClient):
        self.llm_client = llm_client

    def generate_session_report_card(self, playbook_text: str, events: list[dict]) -> dict:
        """
        Analyzes a full session's events to generate a 'Report Card' summary.
        Returns a dict with grading, reasoning, and key takeaways.
        """
        system_prompt = (
            "You are a professional trading performance auditor. "
            "You will be given a trading playbook (strategy) and a list of all adherence/deviation events from a session.\n\n"
            "Your task is to generate a 'Session Report Card' in JSON format with the following keys:\n"
            "1. 'consistency_grade': A letter grade (A, B, C, D, or F) representing how well the trader followed their rules.\n"
            "2. 'summary': A 2-3 sentence overview of their performance.\n"
            "3. 'top_violation': The most critical or frequent rule they broke.\n"
            "4. 'behavioral_pattern': Identify if they showed signs of 'Revenge Trading', 'FOMO', 'Greed', or 'Disciplined' behavior.\n"
            "5. 'actionable_feedback': One specific thing to focus on tomorrow.\n\n"
            "Be firm but objective. Focus on logic adherence, not PnL."
        )

        # Filter for deviations to keep the prompt focused
        deviations = [e for e in events if e.get("type") == "DEVIATION" or (isinstance(e.get("event_data"), dict) and e["event_data"].get("deviation"))]
        
        user_prompt = f"""
        # Playbook:
        {playbook_text}

        # Session Deviations ({len(deviations)} total):
        {json.dumps(deviations[:50], indent=2)} # Top 50 events to avoid context overflow

        Return only the JSON object.
        """

        try:
            response = self.llm_client.generate(system_prompt, user_prompt)
            # Find the JSON block if the LLM added markdown
            if "```json" in response:
                response = response.split("```json")[1].split("```")[0].strip()
            elif "```" in response:
                response = response.split("```")[1].split("```")[0].strip()
            
            return json.loads(response)
        except Exception as e:
            print(f"[REASONER ERROR] Failed to generate report card: {e}")
            return {
                "consistency_grade": "N/A",
                "summary": "Report generation failed due to an internal error.",
                "top_violation": "Unknown",
                "behavioral_pattern": "Indeterminate",
                "actionable_feedback": "Check engine logs for details."
            }

    def explain_deviation(self, playbook_text: str, event_data: dict) -> str:
        """
        Generates a short natural language reasoning explaining why an action 
        resulted in a deviation based on the playbook text and event payload.
        """
        system_prompt = (
            "You are an expert trading algorithmic supervisor. "
            "A trader just took a manual action that triggered a DEVIATION "
            "from their established algorithmic playbook constraints or entry rules.\n\n"
            "Your job is to provide a brief 2-3 sentence markdown explanation of EXACTLY why this is a deviation. "
            "Focus only on what rule they broke (e.g. they bought when price was not below VWAP) or what constraint they violated. "
            "Do NOT provide trading advice. Keep it entirely objective and extremely concise."
        )

        user_prompt = f"""
        # User Strategy / Playbook Details:
        {playbook_text}

        # Market & Evaluation State During Action:
        {json.dumps(event_data, indent=2)}

        Provide a very brief explanation of why this was marked as a deviation, reading from the `deviation_true` array which specifies the failing rules. Keep it to 2-3 sentences max.
        """

        try:
            response = self.llm_client.generate(system_prompt, user_prompt)
            return response.strip()
        except Exception as e:
            return f"Failed to generate reasoning: {e}"
