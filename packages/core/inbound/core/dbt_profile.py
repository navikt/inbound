import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Type

import yaml
from jinja2 import Template
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from inbound.core.environment import get_env
from inbound.core.logging import LOGGER
from inbound.core.models import Profile


class DbtProfileModel(BaseModel):
    elements: Dict[str, Dict[str, Any]]

    @property
    def config(self) -> Dict[str, Any]:
        if "config" in self.elements:
            return self.elements["config"]
        return {}

    def items(self) -> Generator:
        for x in self.elements:
            if x != "config":
                yield {"name": x, "spec": self.elements[x]}

    def get_profile(self, profile_name) -> Dict[str, Any] | None:
        if profile_name in self.elements:
            return self.elements[profile_name]
        return None

    def get_profile_target(self, profile_name) -> Dict[str, Any] | None:
        if profile_name in self.elements:
            return self.elements[profile_name]["target"]
        return None

    def get_connection_spec(self, profile_name) -> Dict[str, Any] | None:
        if profile_name in self.elements:
            target = self.elements[profile_name]["target"]
            spec = self.elements[profile_name]["outputs"][target]
            return spec
        return None


class YamlSettingsSource(PydanticBaseSettingsSource):
    def get_dbt_path(self) -> Path | None:
        profiles_dir = (
            os.getenv("DBT_PROFILES_DIR")
            or self.config.get("profiles_dir")
            or Path.cwd().parent / "dbt"
            or Path.cwd() / "dbt"
        )

        if not (Path(profiles_dir) / "profiles.yml").is_file():
            LOGGER.error(
                f"Error loading dbt profile from {Path(profiles_dir)}. Please provide a path or set the 'DBT_PROFILES_DIR' environment variable"
            )
            return None

        return Path(profiles_dir)

    def get_field_value():
        pass

    def load_yaml_config(self):
        try:
            with open(self.get_dbt_path() / "profiles.yml", "r") as stream:
                return yaml.safe_load(stream)
        except yaml.YAMLError as e:
            LOGGER.info(f"Error loading dbt profile from {self.get_dbt_path()}")

    def get_dbt_profile(self) -> DbtProfileModel:
        path = self.get_dbt_path()
        LOGGER.info(f"Loading dbt profile from {path}")
        try:
            profile_json = self.load_yaml_config()
            # Replace 'env_var's in template
            temp = Template(json.dumps(profile_json)).render(env_var=get_env)
            final_json = json.loads(temp, strict=False)

            return DbtProfileModel(elements=final_json)
        except Exception as e:
            LOGGER.error(f"Error parsing dbt profile from {path}. {e}")

    def __call__(self) -> Dict[str, DbtProfileModel]:
        model = self.get_dbt_profile()
        return {"profile": model}


class DbtProfile(BaseSettings):
    profile: Optional[DbtProfileModel] = None
    profile_name: Optional[str] = None
    profile_target: Optional[str] = None
    profiles_dir: Optional[str] = None

    model_config: SettingsConfigDict(
        # extra="allow",
        default_profiles_dir=Path.home() / ".dbt",
        env_prefix="DBT_",
        env_nested_delimiter="__",
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


def dbt_connection_params(
    profile: str, target: None, profiles_dir: str | None = None, *args, **kwargs
) -> Dict[str, Any] | None:
    profiles = DbtProfile(
        profile_name=profile, profile_target=target, profiles_dir=profiles_dir
    ).profile.elements
    LOGGER.info(f"Loaded profiles {str(list(profiles.keys()))}")
    if not profiles:
        LOGGER.error(
            f"Profile with name {profile} and target {target} not found in profile_dir {profiles_dir}"
        )
        return {}
    LOGGER.info(f"Loading target: {target} from profile {profile}")
    target = target or profiles[profile]["target"] or "dev"
    LOGGER.info(f"Loading profile {profile}. Target: {target}")
    params = profiles[profile]["outputs"][target]
    return params


def dbt_config(profiles_dir: str = None) -> Dict[str, Any]:
    profiles = DbtProfile(profiles_dir=profiles_dir).profile.elements
    if not profiles:
        return {}
    try:
        config = profiles["config"]
        return config
    except:
        return {}
