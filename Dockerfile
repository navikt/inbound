FROM python:3.11-slim-bookworm AS base

RUN apt-get update
# install odbc driver
RUN apt-get install -y freetds-common tdsodbc git

# innstall python packages
WORKDIR /
RUN python3 -m venv /venv --without-pip
COPY . .
RUN pip --python /venv/bin/python install .[snowflake,oracle,mainmanager,anaplan,mssql]

ENV PYTHONPYCACHEPREFIX="/tmp/.pycache"
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1
# Set Python path
ENV PYTHONPATH="/venv/lib/python3.11/site-packages"
ENV PATH="/venv/bin:${PATH}"
