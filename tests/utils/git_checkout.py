from snowflake.sqlalchemy import URL

from inbound.core.dbt_profile import DbtProfile, dbt_connection_params
from inbound.core.logging import LOGGER
from inbound.core.models import Profile, Spec
from inbound.snowflake import SnowflakeConnection

spec = Spec(profile="test-snowflake-db", target="dev")
profile = Profile(type="snowflake", name=f"snowflake", spec=spec)

with SnowflakeConnection(profile=profile) as db:
    # res = db.to_pandas("select current_version()")

    resultproxy = db.execute("select current_version()")

    d, a = {}, []
    for rowproxy in resultproxy:
        # rowproxy.items() returns an array like [(key0, value0), (key1, value1)]
        for column, value in rowproxy.items():
            # build up the dictionary
            d = {**d, **{column: value}}
        a.append(d)

    print(a)
