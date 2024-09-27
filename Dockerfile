FROM python:3.11-slim-bookworm@sha256:fc39d2e68b554c3f0a5cb8a776280c0b3d73b4c04b83dbade835e2a171ca27ef AS base
RUN apt-get update

RUN apt-get install -y git

# innstall python packages
WORKDIR /
RUN python3 -m venv /venv --without-pip
COPY . .
RUN pip --python /venv/bin/python install .
# Set Python path
ENV PATH="/venv/bin:${PATH}"
