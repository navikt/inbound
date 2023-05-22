from enum import Enum
from pathlib import Path
from typing import Sequence, Union

import click

source_path = Path(__file__).parent


def init_template_project(path: Union[str, Path] = Path.cwd()) -> None:

    root_path = Path(path)
    config_path = root_path / f"pyproject.toml"
    dbt_path = root_path / "dbt"
    inbound_path = root_path / "inbound"
    tests_path = root_path / "tests"

    if config_path.exists():
        raise click.ClickException(f"Found an existing project in '{config_path}'")

    _create_config(config_path)
    _create_empty_folders([dbt_path, inbound_path])
    _create_tests(tests_path)


def _write_file(path: Path, source: str) -> None:
    with open(source_path / source) as fs:
        payload = fs.read()
        with open(path, "w", encoding="utf-8") as fd:
            fd.write(payload)


def _create_empty_folders(target_folders: Sequence[Path]) -> None:
    for folder_path in target_folders:
        folder_path.mkdir(exist_ok=True)
        (folder_path / ".gitkeep").touch()


def _create_config(config_path: Path) -> None:
    _write_file(config_path, "pyproject.toml")


def _create_tests(tests_path: Path) -> None:
    tests_path.mkdir(exist_ok=True)
    _write_file(tests_path / "test_duckdb.py", "test_duckdb.py")
