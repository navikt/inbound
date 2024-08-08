# inbound
En pakke for å laste dataprodukter fra ulike kildesystemer til Snowflake.

## Installering

```shell
pip install inbound
```

eller

```shell
pip install inbound@git+https://github.com/navikt/inbound.git
```

## Release av ny versjon

Vi bruker [GitHub Release](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository) til versjonering. Vi forholder oss til [semver](https://semver.org) versjonering. `<major>.<minor>.<patch>` Eks: 0.1.0. Siden vi enda ikke er på versjon 1 kan `minor` inkrementeres med 1 ved breaking changes i apiet og `patch` ved nye features eller bug fiks.

For å release en ny versjon må en gjøre følgende:
* Merge koden til main
* Oppdatere `version` i [setup.py](setup.py)
* Opprett `<major>.<minor>` tag. Eks: `git tag v0.2`
* Opprett `<major>.<minor>.<patch>` tag. Eks: `git tag v0.2.0`
* Push tags til github med: `git push origin v0.2 v0.2.0`
* Opprett ny release på [github](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository) og velg den nye `<major>.<minor>.<patch>` taggen.
