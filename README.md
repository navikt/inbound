# inbound
En pakke for å laste dataprodukter fra ulike kildesystemer til Snowflake.

## Installering

```shell
make install 
```

## Release av ny versjon

Vi bruker [GitHub Release](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository) til versjonering og bygg av nytt docker-image. Versjonsnummereringen skal følge [semver](https://semver.org): `<major>.<minor>.<patch>` Eks: `0.1.0`. Siden vi enda ikke er på versjon 1 kan `minor` inkrementeres med 1 ved breaking changes i apiet og `patch` ved nye features eller bug fiks.

For å release en ny versjon må en gjøre følgende:
* Merge koden til main
* Oppdatere `version` i [setup.py](setup.py)
* Opprett/oppdater `<major>.<minor>` tag. Eks: `git tag -f v0.2`
* Opprett `<major>.<minor>.<patch>` tag. Eks: `git tag v0.2.0` (tagen skal ikke eksistere fra før)
* Push tags til github med: `git push -f origin v0.2 v0.2.0`
* Opprett ny release på [github](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository)
    * Steg 1: Velg den nye `<major>.<minor>.<patch>` taggen.
    * Steg 2: Trykk Generate release notes for å få utfylt relevant informasjon
    * Steg 3: Trykk Publish release.

## Kjøring av integrasjonstester

For å kunne kjøre integrasjonstestene må testdatabasene startes opp først. Dette gjøres med `docker compose` når en står i `inbound`-mappen

Starte testdatabaser

```shell
docker compose up
```

"Rive" testdatabase

```shell
docker compose down
```

For at integrasjonstestene for mssql skal fungere må du ha installert UnixODBC driver og Microsoft ODBC driver for SQL Server. Slik kan du installere disse på MacOS:

```shell
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
```

Oppskrift fra [Hailiang Chen på Medium](https://medium.com/@chen19/accessing-ms-sql-server-on-macos-via-odbc-a-step-by-step-guide-86cb5c70ba14)