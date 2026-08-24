# Streams a completion through the scope gateway using the official OpenAI
# SDK — the only scope-specific line is base_url.
#
#   pip install openai && python examples/streaming_demo.py

from openai import OpenAI

client = OpenAI(base_url="http://localhost:8090/v1", api_key="unused-for-now")

stream = client.chat.completions.create(
    model="llama3.2:1b",
    messages=[{"role": "user", "content": "In one sentence, what is a write-ahead log?"}],
    stream=True,
)

for event in stream:
    delta = event.choices[0].delta.content
    if delta:
        print(delta, end="", flush=True)
print()
