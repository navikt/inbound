import os
from pathlib import Path


def test_env():
    assert Path(os.environ["INBOUND_PROJECT_DIR"]).is_dir()


def test_dbt_profile():
    assert Path(os.environ["DBT_PROFILES_DIR"]).is_dir()
