import json

import yaml

import inbound.core.dbt_profile as dbt_profile
from inbound.core.logging import LOGGER
from inbound.core.models import Profile, SodaProfile, SodaSpec, Spec


def soda_profile(
    data_source: str, profile_name: str, target: str = "dev", profiles_dir: str = None
) -> SodaProfile:
    try:
        params: dict = dbt_profile.dbt_connection_params(
            profile_name, target, profiles_dir
        )
        spec = Spec(**params)
        soda_spec = SodaSpec(
            type=spec.type,
            account=spec.account,
            username=spec.user,
            password=spec.password,
            role=spec.role,
            database=spec.database,
            warehouse=spec.warehouse,
        )
        if soda_spec.host is None and soda_spec.type == "snowflake":
            soda_spec.host = f"https://{soda_spec.account}.snowflakecomputing.com"

        profile = SodaProfile.parse_obj({data_source: soda_spec})
        return profile
    except Exception as e:
        LOGGER.info("Error loading soda connection str")
        return None


def soda_profile_yml(
    data_source: str, profile_name: str, target: str = "dev", profiles_dir: str = None
) -> str:
    try:
        profile = soda_profile(data_source, profile_name, target, profiles_dir)
        return yaml.dump(json.loads(profile.json()))
    except Exception as e:
        LOGGER.info("Error writing soda profile yaml string")
        return None


def get_soda_profile(profile: Profile) -> str:
    try:
        spec = profile.spec
        soda_spec = SodaSpec(
            type=spec.type,
            account=spec.account,
            username=spec.user,
            password=spec.password,
            role=spec.role,
            database=spec.database,
            warehouse=spec.warehouse,
        )
        if soda_spec.host is None and soda_spec.type == "snowflake":
            soda_spec.host = f"https://{soda_spec.account}.snowflakecomputing.com"

        soda_profile = SodaProfile.parse_obj(
            {f"data_source {profile.spec.database}": soda_spec}
        )
    except Exception as e:
        LOGGER.info("Error loading soda connection str")

    try:
        return yaml.dump(json.loads(soda_profile.json()))
    except Exception as e:
        LOGGER.info("Error writing soda profile yaml string")
        return None
