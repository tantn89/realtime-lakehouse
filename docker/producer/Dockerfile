# Small image for the Wikimedia -> Kafka producer (no Spark, no Java).
FROM python:3.11-slim-bookworm

RUN useradd --create-home --uid 1000 producer
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY data/recordings/sample.jsonl ./data/recordings/sample.jsonl
RUN pip install --no-cache-dir ".[producer]" && mkdir -p /state && chown -R producer /app /state

USER producer
ENTRYPOINT ["python", "-m", "wiki_lakehouse.producer"]
CMD ["live"]
