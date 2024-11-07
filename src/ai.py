import os
from openai import OpenAI
import httpx
from dotenv import load_dotenv

load_dotenv()

def url_to_md(url : str) -> str:
    # clean url
    url = url.strip()
    url = "https://r.jina.ai/" + url
    resp = httpx.get(url)
    resp.raise_for_status()
    return resp.text

class LLM:
    def __init__(self):
        self.openai_key = os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=self.openai_key)

    def ask(self, 
                chat_history: list[str],
                model: str = "gpt-4o",
                max_tokens: int = 1024,
                json_mode: bool = False,
                ):
        client = self.client
        # if msg is not str, then it's a map {role: "user" | "assistant", content: str}, just append it
        # if msg is str, then it's {"role": "user", "content": str}
        # if msg is {"role": "user", "content": str}, then just append it
        history = []
        for msg in chat_history:
            if isinstance(msg, str):
                history.append({"role": "user", "content": msg})
            else:
                history.append(msg)

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
                *history,
            ],
            max_tokens=max_tokens,
            response_format={"type": "json_object"} if json_mode else None,
        )
        return {"content": response.choices[0].message.content, "role": "assistant"}

if __name__ == "__main__":
    llm = LLM()
    # read from stdin, make a conversation
    chat_history = []
    while True:
        line = input("user> ")
        if line == "":
            break
        if line.startswith("http"):
            line = url_to_md(line) + f"\n\n---\n\nhere is the markdown content of the url: {line}, wait for the user to ask about it in next line"
        chat_history.append({"role": "user", "content": line})
        resp = llm.ask(chat_history)
        print("assistant> ", resp["content"])
        chat_history.append(resp)
