FROM python:3.11-slim-bookworm@sha256:fc39d2e68b554c3f0a5cb8a776280c0b3d73b4c04b83dbade835e2a171ca27ef AS base

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

# Teste etter på...

FROM gcr.io/distroless/python3-debian12:debug as final

# copy tdsodbc files
COPY --from=base /usr/lib/x86_64-linux-gnu /usr/lib/x86_64-linux-gnu
COPY --from=base /etc/odbcinst.ini /etc/odbcinst.ini

# should copy these instead of the hole lib directory but I can't find the last libs that libtdsodbc.so needs
#COPY --from=freetds-builder /usr/lib/x86_64-linux-gnu/libodbc* /usr/lib/x86_64-linux-gnu/
#COPY --from=freetds-builder /usr/lib/x86_64-linux-gnu/libltdl* /usr/lib/x86_64-linux-gnu/
#COPY --from=freetds-builder /usr/lib/x86_64-linux-gnu/odbc /usr/lib/x86_64-linux-gnu/odbc/


ENV PYTHONPYCACHEPREFIX="/tmp/.pycache"
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1
# Set Python path
ENV PYTHONPATH="/app/venv/lib/python3.11/site-packages"
ENV PATH="/app/venv/bin:${PATH}"
