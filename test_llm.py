import requests, os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(".env")

API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
LLM_NAME = os.getenv("LLM_NAME")

client = OpenAI(base_url=API_URL, api_key=API_KEY)
completion = client.chat.completions.create(
    extra_body={},
    model=LLM_NAME,
    temperature=0.2,
    messages=[
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "Сколько будет 2+2?"
                }
            ]
        }
    ]
)

print(completion.choices[0].message.content)