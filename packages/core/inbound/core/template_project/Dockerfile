FROM python:3.10-slim as base

ENV POETRY_VERSION="1.4.2" \
    POETRY_HOME="/opt/poetry" \
    VIRTUAL_ENVIRONMENT_PATH="/.venv" 

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential \
        curl \
        git \
    && curl -sSL https://install.python-poetry.org | python - \
    && poetry --version \
    && apt-get purge --auto-remove -y \
      build-essential \
      curl 


#################################################################
FROM base as builder

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1


RUN python3 -m venv ${VIRTUAL_ENVIRONMENT_PATH}

COPY ./pyproject.toml ./
# innstall python packages

RUN	poetry env use ${VIRTUAL_ENVIRONMENT_PATH}/bin/python
RUN poetry install --no-interaction


#################################################################
FROM node:14-slim

EXPOSE 8080

# TODO drop use settings
ENV DBT_DIR="/app/dbt" \
    DBT_PROFILES_DIR="/app/dbt" \
    INBOUND_PROFILES_DIR="/app/inbound" \
    INBOUND_JOBS_DIR="/app/inbound/jobs" \
    INBOUND_GCS_BUCKET="artifacts_dev"

COPY --from=builder $VIRTUAL_ENVIRONMENT_PATH $VIRTUAL_ENVIRONMENT_PATH

ENV PATH="$VIRTUAL_ENVIRONMENT_PATH/bin:$PATH"

# Copy dbt profile to default dbt profiles directory
COPY dbt/profiles.yml /home/.dbt/profiles.yml

# Copy soda config to default soda config directory
COPY soda/config.yml /home/.soda/config.yml

COPY ./api /app/api
COPY ./apps /app/apps
COPY ./dbt /app/dbt
COPY ./inbound /app/inbound
COPY ./soda /app/soda
COPY ./streamlit /app/streamlit
COPY ./main.py /app

COPY ./soda/config.yml /root/.soda/config.yml

WORKDIR /app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]