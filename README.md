# Inbound 

## Getting started 

Create a folder for you new project

```shell
mkdir my_project
```

Create a virtual environment 

```shell
python -m venv .venv && source ./.venv/bin/activate
```


Intialize your project

```shell
pip install -e 'git+https://github.com/navikt/inbound.git@main#egg=inbound-core&subdirectory=packages/core'
inbound init
```

Install dependencies

```shell
poetry install
```

Run tests

```shell
poetry run pytest ./tests
```

## Development

Clone repo


```shell
make install
```

### Unit testing

```shell
make test
```


## Next up

test using bucket storage as intermediate beween source and sink instead of pandas

create parallel mode in inbound.cors.jobs.py

complete getting started template project (core/template_project)
