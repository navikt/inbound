import re
from importlib import metadata
from pathlib import Path

SOURCE_DIR = Path(__file__)


def get_package_version():
    return metadata.metadata("inbound-core")["Version"]


def get_pacage_name():
    return metadata.metadata("inbound-core")["Name"]
