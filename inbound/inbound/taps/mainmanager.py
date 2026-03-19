import json
from typing import Any, Generator
from urllib.parse import urljoin

import requests

from ..core.models import Description
from ..sdk.tap import Tap

class MainManagerError(Exception):
    pass

class MainManagerDataSupplier:
    def __init__(
        self,
        table: str,
        username: str,
        password: str,
        base_url: str = "https://nav-test.mainmanager.no",
    ):
        self.table = table
        self.username = username
        self.password = password
        self.base_url = base_url
        self.endpoint = f"/api/v1/datawarehouseview?viewname={table}&datatableoutput=4)"

    def get_data(self):
        token = self._get_mainmanager_token()
        headers = {"Authorization": f"Bearer {token}"}

        response = requests.get(urljoin(self.base_url, self.endpoint), headers=headers)

        payload = response.json()
        if payload['Success'] is False:
            raise MainManagerError(payload['Message'])
        return response.json()

    # Autentisering mot MainManager-APIet
    def _get_mainmanager_token(self):
        data = {
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
        }

        response = requests.post(urljoin(self.base_url, "/restapi/token"), data=data)

        payload = response.json()
        if response.status_code != 200:
            raise MainManagerError(payload['error_description'])
        return response.json().get("access_token")


class MainManagerTap(Tap):

    def __init__(
        self,
        table: str,
        username: str,
        password: str,
        base_url: str = "https://nav-test.mainmanager.no",
        data_supplier=None,
    ):
        self.data_supplier = data_supplier
        if data_supplier is None:
            self.data_supplier = MainManagerDataSupplier(
                table=table, username=username, password=password, base_url=base_url
            )

    def column_descriptions(self) -> list[Description]:
        return [
            Description(
                name="raw", type="variant", precision=None, scale=None, nullable=True
            )
        ]

    # Henter ut json-data fra APIet og deler opp i én record per rad
    def data_generator(self) -> Generator[list[tuple], Any, None]:

        eiendomsdata = self.data_supplier.get_data()

        # TODO: vurdere om informasjon om antall rader skal være med til snowflake
        # print('Antall records hentet fra MainManager-API: ', str(eiendomsdata['TotalNumberOfRecords']))

        table_content = json.loads(eiendomsdata.get("DataTable"))

        yield [(json.dumps(item),) for item in table_content["table"]]
