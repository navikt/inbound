import numpy as np
import pandas as pd

from inbound.core.models import Profile, Spec
from inbound.duckdb import DuckDBConnection

df = pd.DataFrame(
    {
        "first": np.random.rand(100).tolist(),
        "second": np.random.randint(100, size=100).tolist(),
        "third": np.random.choice(["a", "b", "c", "d"], size=100).tolist(),
    }
)

credentials = Spec(name="duckdb", database=None, table="test")
profile = Profile(spec=credentials)


def test_write_pandas_append():
    # split in 4 chunks
    chunks = np.array_split(df, 4)
    with DuckDBConnection(profile=profile) as db:
        for index in range(len(chunks)):
            ret, job_res = db.from_pandas(chunks[index], chunk_number=index)
            assert job_res.result == "DONE"


def test_roundtrip():
    with DuckDBConnection(profile=profile) as db:
        db.from_pandas(df)
        res_iterator = db.to_pandas()
        df_res, job_res = next(res_iterator)

        assert job_res.result == "DONE"
        assert df_res.size > 0


def test_pandas_replace():
    with DuckDBConnection(profile=profile) as db:
        ret, job_res = db.from_pandas(df)

        assert job_res.result == "DONE"


def test_drop_table():
    with DuckDBConnection(profile=profile) as db:
        db.from_pandas(df)
        ret = db.drop(profile.spec.table)

        assert ret.result == "DONE"
