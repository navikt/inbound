import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import yaml
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from inbound.core.logging import LOGGER


class BookmarkModel(BaseModel):
    config: str


class GCPModel(BaseModel):
    project_id: str
    secrets: Optional[List] = None
    syncbucket: Optional[str] = None
    metadatabucket: Optional[str] = None


class SpecModel(BaseModel):
    secrets_path: Optional[str] = None
    gcp: Optional[GCPModel] = None
    bookmark: Optional[BookmarkModel] = None


class MetadataModel(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class DBTModel(BaseModel):
    profiles_dir: Optional[str] = None
    profile: Optional[str] = None
    target: str


class TagModel(BaseModel):
    name: Optional[str] = None
    policy: Optional[str] = None


class LoggingModel(BaseModel):
    local_path: Optional[str] = None
    upload: Optional[bool] = None
    profile: Optional[str] = None


class YamlSettingsSource(BaseSettings):
    def get_inbound_path(self) -> Path:
        if os.getenv("INBOUND_PROJECT_DIR"):
            return os.getenv("INBOUND_PROJECT_DIR")
        if Path(Path.home() / ".inbound").exists():
            return Path.home() / ".inbound"
        else:
            return Path.cwd() / "inbound"

    def load_yaml_config(self, path: str | None = None):
        if not (Path(path) / "inbound_project.yml").is_file():
            LOGGER.info(f"Error loading settings from {path}")
            raise ValueError(f"Error loading settings from {path}")

        with open(Path(path) / "inbound_project.yml", "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as e:
                LOGGER.info(f"Error loading settings from {path}")

    def __call__(self) -> Dict[str, Any]:
        path = self.get_inbound_path()
        settings_json = self.load_yaml_config(path)
        return settings_json


class Settings(BaseSettings):
    kind: Optional[str] = None
    spec: Optional[SpecModel] = None
    metadata: Optional[MetadataModel] = None
    version: Optional[str] = None
    logging: Optional[LoggingModel] = None
    dbt: Optional[DBTModel] = None
    tags: Union[List[TagModel], TagModel, None] = None

    model_config = SettingsConfigDict(
        extra="allow", env_prefix="INBOUND_", env_nested_delimiter="__"
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            YamlSettingsSource(settings_cls),
            file_secret_settings,
        )
