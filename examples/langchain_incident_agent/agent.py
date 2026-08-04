import sys

from langchain.agents import create_agent
from langchain_ollama import ChatOllama
from tools import create_datahub_incident

MODEL = "qwen3:8b"

SYSTEM_PROMPT = """You are a data reliability assistant.

When a user reports that a dataset or pipeline is unhealthy, file an incident in
DataHub using the create_datahub_incident tool. Pick the incident_type that best
matches the symptom: stale or late data is FRESHNESS, an unexpected row count is
VOLUME, a pipeline or job failure is OPERATIONAL.

The tool needs a full dataset URN. If the user only gives you a name, say so and
ask for the URN instead of guessing one.
"""


def run_incident_agent(query: str) -> str:

    llm = ChatOllama(model=MODEL)

    agent = create_agent(
        model=llm,
        tools=[create_datahub_incident],
        system_prompt=SYSTEM_PROMPT,
    )

    print(f"Executing agent for query: {query}")
    result = agent.invoke({"messages": [{"role": "user", "content": query}]})
    return result["messages"][-1].content


if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_query = " ".join(sys.argv[1:])
    else:
        test_query = (
            "The dataset urn:li:dataset:(urn:li:dataPlatform:snowflake,"
            "showcase.ecommerce.orders,PROD) is stale. Please log an incident."
        )

    print(run_incident_agent(test_query))
