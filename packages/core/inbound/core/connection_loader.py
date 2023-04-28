import importlib
import pkgutil
from typing import List

from inbound.core.logging import LOGGER


class PluginInterface:
    """Represents a plugin interface. A plugin must provide a register function."""

    @staticmethod
    def register() -> None:
        """Register the plugin."""


def import_module(name: str) -> PluginInterface:
    """Imports a module given a name."""
    return importlib.import_module(name, package=__name__)  # type: ignore


def load_plugins(plugins: List[str]) -> None:
    """Loads the plugins defined in the plugins list."""
    loadedPlugins: List[str] = []

    # plugins installed by default
    for plugin_name in plugins:
        if not plugin_name in loadedPlugins:
            try:
                plugin = import_module(f"inbound.{plugin_name}.{plugin_name}")
                plugin.register()
                loadedPlugins.append(plugin_name)
            except Exception as e:
                LOGGER.info(f"Plugin {plugin_name} not installed locally")

    # plugins installed separetely
    for finder, name, ispkg in pkgutil.iter_modules():
        LOGGER.info(name, finder, ispkg)
        if name.startswith("inbound"):
            try:
                plugin = import_module(name)
                plugin.register()
                loadedPlugins.append(name)
            except Exception as e:
                LOGGER.error(f"Error loading plugin {name}. {e}")
