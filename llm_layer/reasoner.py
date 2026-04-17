from llm_layer.openai_client import OpenAILLMClient
import json

class DeviationReasoner:
    def __init__(self, llm_client: OpenAILLMClient):
        self.llm_client = llm_client

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
