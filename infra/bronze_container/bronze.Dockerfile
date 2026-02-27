FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POETRY_VERSION=2.1.4

WORKDIR /app

RUN apt-get update && \
    apt-get install -y curl build-essential && \
    apt-get clean

RUN pip install "poetry==$POETRY_VERSION"

RUN poetry config virtualenvs.create false

COPY pyproject.toml ./
COPY poetry.lock ./
RUN poetry install --no-root --only main

COPY data_pipeline ./data_pipeline
COPY .env ./
COPY scripts/bronze.py ./bronze.py

ENV SPARK_VERSION=3.5

ENTRYPOINT ["python"]
CMD ["bronze.py"]
