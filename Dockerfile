FROM python:3.11-slim-trixie@sha256:8ef21a26e7c342e978a68cf2d6b07627885930530064f572f432ea422a8c0907 AS base

FROM base AS freetds-builder
RUN apt-get update
# install odbc driver
RUN apt-get install -y freetds-common tdsodbc

FROM base AS pybuilder
RUN apt-get update

RUN apt-get install -y git

# innstall python packages
WORKDIR /
RUN python3 -m venv /venv --without-pip
COPY . .
RUN pip --python /venv/bin/python install .[snowflake,oracle,mainmanager,anaplan,mssql]

FROM gcr.io/distroless/python3-debian12:latest as final

# copy tdsodbc files
COPY --from=freetds-builder /usr/lib/x86_64-linux-gnu /usr/lib/x86_64-linux-gnu
COPY --from=freetds-builder /etc/odbcinst.ini /etc/odbcinst.ini

# copy venv
COPY --from=pybuilder /venv /venv

ENV PYTHONPYCACHEPREFIX="/tmp/.pycache"
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1
# Set Python path
ENV PYTHONPATH="/venv/lib/python3.11/site-packages"
ENV PATH="/venv/bin:${PATH}"
