
SHELL = /bin/bash
.DEFAULT_GOAL = install

VENV = ./.venv/bin/activate
PY = ./.venv/bin/python -m

.PHONY: install ## install requirements in virtual env
install:
	rm -rf .venv
	python3.11 -m venv .venv && \
		${PY} pip install --upgrade pip && \
		${PY} pip install -e .[oracle,mssql,snowflake,dev]

start-colima:
	colima start --arch x86_64 --memory 6

stop-colima:
	colima stop
