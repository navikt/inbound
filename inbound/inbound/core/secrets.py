import logging

from dotenv import load_dotenv

logger = logging.getLogger("dataproduct")


def set_env_variables_from_secrets() -> None:
    # setup local environment variables from .env
    local_path = ".env"
    local_env_loaded = load_dotenv()
    if local_env_loaded:
        logger.info(f"loaded env variables from {local_path}")
    else:
        logger.warning(f"could not load env variables from {local_path}")

    vault_path = "/var/run/secrets/nais.io/vault/app.env"
    vault_env_loaded = load_dotenv(dotenv_path=vault_path)
    if vault_env_loaded:
        logger.info(f"loaded env variables from {vault_path}")
    else:
        logger.warning(f"could not load env variables from {vault_path}")
