from llm_layer.llm_client import LLMClient
from openai import OpenAI
from dotenv import load_dotenv
import os

load_dotenv("../.env")
class OpenAILLMClient(LLMClient):
    def __init__(self, model: str = "gpt-4o-mini"):
        self.client = OpenAI(api_key=os.getenv("OPENAI_KEY"))
        self.model = model

    def generate(self, system_prompt: str, user_prompt: str) -> str:

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0
        )
        return response.choices[0].message.content

    def generate_chat(self, system_prompt: str, chat_history: list) -> str:
        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(chat_history)
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0
        )
        return response.choices[0].message.content

    def stream_chat(self, system_prompt: str, chat_history: list):
        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(chat_history)
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0,
            stream=True
        )
        for chunk in response:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
