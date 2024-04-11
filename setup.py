from setuptools import find_packages, setup

with open("inbound/README.md", "r") as f:
    long_description = f.read()

setup(
    name="inbound",
    version="0.0.1",
    description=(
        "Pakke for å laste dataprodukter fra ulike kildesystemer til Snowflake"
    ),
    # package_dir={"": "inbound"},
    packages=find_packages(include=("inbound/*,")),
    author="NAV IT Virksomhetsdatalaget",
    author_email="virksomhetsdatalaget@nav.no",
    url="https://github.com/navikt/vdl-regnskapsdata/tree/main/inbound",
    license="MIT",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    install_requires=["oracledb>=2.0.0", "pyodbc", "jinja2", "dotenv", "uuid"],
    python_requires=">=3.11",
)
