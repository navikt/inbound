#FROM python:3.11-slim-bookworm@sha256:fc39d2e68b554c3f0a5cb8a776280c0b3d73b4c04b83dbade835e2a171ca27ef AS base
FROM python:3.11-slim-bookworm AS base

RUN apt-get update && apt-get install -y curl apt-transport-https gnupg2 software-properties-common build-essential
# Download the package to configure the Microsoft repo
RUN curl -sSL -O https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb
# Install the package
RUN dpkg -i packages-microsoft-prod.deb
# Delete the file
RUN rm packages-microsoft-prod.deb

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
# optional: for bcp and sqlcmd
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools18
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
ENV PATH="$PATH:/opt/mssql-tools18/bin"
# optional: for unixODBC development headers
RUN apt-get install -y unixodbc-dev

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

## Teste etter på...

#FROM gcr.io/distroless/python3-debian12@sha256:8ce6bba3f793ba7d834467dfe18983c42f9b223604970273e9e3a22b1891fc27 AS final

## copy tdsodbc files
#COPY --from=base /usr/lib/x86_64-linux-gnu /usr/lib/x86_64-linux-gnu
#COPY --from=base /etc/odbcinst.ini /etc/odbcinst.ini

## should copy these instead of the hole lib directory but I can't find the last libs that libtdsodbc.so needs
##COPY --from=freetds-builder /usr/lib/x86_64-linux-gnu/libodbc* /usr/lib/x86_64-linux-gnu/
##COPY --from=freetds-builder /usr/lib/x86_64-linux-gnu/libltdl* /usr/lib/x86_64-linux-gnu/
##COPY --from=freetds-builder /usr/lib/x86_64-linux-gnu/odbc /usr/lib/x86_64-linux-gnu/odbc/

## copy python venv
#COPY --from=base /venv /venv

## install shell
#COPY --from=base /bin/sh /bin/sh

#ENV PYTHONPYCACHEPREFIX="/tmp/.pycache"
## Turns off buffering for easier container logging
#ENV PYTHONUNBUFFERED=1
## Set Python path
#ENV PYTHONPATH="/venv/lib/python3.11/site-packages"
#ENV PATH="/venv/bin:${PATH}"
