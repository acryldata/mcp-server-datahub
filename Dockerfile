FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
COPY README.md .
COPY src ./src

RUN pip install --upgrade pip && pip install uv && uv sync

ENV TRANSPORT="stdio"

EXPOSE 8000

CMD [ "sh", "-c", "uv run mcp-server-datahub --transport ${TRANSPORT}" ]
