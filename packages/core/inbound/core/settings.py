import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, BaseSettings

from inbound.core.logging import LOGGER


class BookmarkModel(BaseModel):
    config: str


class GCPModel(BaseModel):
    project_id: str
    secrets: Optional[List]
    syncbucket: Optional[str]
    metadatabucket: Optional[str]


class SpecModel(BaseModel):
    secrets_path: Optional[str] = None
    gcp: Optional[GCPModel] = None
    bookmark: Optional[BookmarkModel] = None


class MetadataModel(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class DBTModel(BaseModel):
    profiles_dir: Optional[str]
    profile: Optional[str]
    target: str


class TagModel(BaseModel):
    name: Optional[str]
    policy: Optional[str]


class LoggingModel(BaseModel):
    local_path: Optional[str]
    upload: Optional[bool]
    profile: Optional[str]


class InboundModel(BaseModel):
    spec: Optional[SpecModel]
    metadata: Optional[MetadataModel]
    version: str
    logging: Optional[LoggingModel]
    dbt: Optional[DBTModel]
    tags: Optional[TagModel]


class Settings(BaseSettings):
    spec: Optional[SpecModel]
    metadata: Optional[MetadataModel]
    version: Optional[str]
    logging: Optional[LoggingModel]
    dbt: Optional[DBTModel]
    tags: Optional[TagModel]

    class Config:

        default_settings_path = Path.home() / ".inbound"
        if not Path(default_settings_path).exists():
            default_settings_path = Path.cwd() / "inbound"
        env_prefix = "INBOUND_"
        env_nested_delimiter = "__"
        fields = {"secret_dir": {"env": "INBOUND_SECRETS_DIR"}}

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                env_settings,
                _yml_config_settings_source,
                file_secret_settings,
            )


def _yml_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    path = os.getenv("INBOUND_PROJECT_DIR", settings.__config__.default_settings_path)
    settings_json = _load_yaml_config(path)
    model = InboundModel(**settings_json)
    return dict(model)


def _load_yaml_config(path: str | None = None):
    env_variable = "INBOUND_PROJECT_DIR"
    if path is None:
        path = os.getenv(env_variable)

    if path is None:
        LOGGER.info(f"Please provide environment variable {env_variable}")
        raise ValueError(f"Error getting env variable {env_variable}")

    if not (Path(path) / "inbound_project.yml").is_file():
        LOGGER.info(f"Error loading settings from {path}")
        raise ValueError(f"Error loading settings from {path}")

    with open(Path(path) / "inbound_project.yml", "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as e:
            LOGGER.info(f"Error loading settings from {path}")
