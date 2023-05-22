# Inbound 

## Getting started 

Create a folder for you new project

```shell
mkdir my_project
```

Create a virtual environment 

```shell
python -m venv .venv
source ./.venv/bin/activate
```


Intialize your project

```shell
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


```shell

make install
```

### Unit testing

```shell
make test
```