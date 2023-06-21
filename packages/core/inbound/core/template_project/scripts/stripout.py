import os
from pathlib import Path

import nbstripout


def stripout():
    path = Path("notebooks")
    for p in path.rglob("*"):
        if p.suffix == ".ipynb":
            os.system(f"nbstripout {p.absolute()}")


if __name__ == "__main__":
    stripout()
