
SHELL = /bin/bash
.DEFAULT_GOAL = format
pkg_src = packages
tests_src = tests
ifeq ($(OS),Windows_NT)
	BIN = Scripts
else
	BIN = bin
endif

VENV = ./.venv/$(BIN)/activate
PY = ./.venv/$(BIN)/python -m  

isort = $(PY) isort $(pkg_src) $(tests_src)
black = $(PY) black $(pkg_src) $(tests_src)
flake8 = $(PY) flake8 $(pkg_src) $(tests_src)

build:
	rm -r -f dist
	${PY} build --wheel --outdir dist/
	twine check ./dist/*

## install in virtal env 
install:
	python3.11 -m venv .venv && \
		${PY} pip install --upgrade pip && \
		poetry env use ./.venv/bin/python
		poetry install


## Perform the most common development-time rules
all: format lint test

## Auto-format the source code (isort, black)
format:
	$(isort)
	$(black)

## Run unit tests
test:
	poetry run pytest ./tests/unit --cov=$(pkg_src)

## Start dev containers
dockerup:
	docker-compose -f ./tests/e2e/docker-compose.yml up 

## Stop dev containers
dockerdown:
	docker-compose -f ./tests/e2e/docker-compose.yml down 

## Run end-to-end tests
e2e:
	poetry run pytest ./tests/e2e/postgres/
	poetry run pytest ./tests/e2e/oracle/
	poetry run pytest ./tests/e2e/snowflake/
	poetry run pytest ./tests/e2e/metadata/

## Run tests, generate a coverage report, and open in browser
testcov:
	pytest ./tests/unit --cov=$(pkg_src)
	@echo "building coverage html"
	@coverage html
	@echo "opening coverage html in browser"
	@open htmlcov/index.html
