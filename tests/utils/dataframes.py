import numpy as np
import pandas as pd

df = pd.DataFrame(
    {
        "first": np.random.rand(100).tolist(),
        "second": np.random.randint(100, size=100).tolist(),
        "third": np.random.choice(["a", "b", "c", "d"], size=100).tolist(),
    }
)
df.index.name = "p"
df2 = pd.DataFrame(
    {
        "fourth": np.random.rand(100).tolist(),
        "fifth": np.random.randint(100, size=100).tolist(),
        "sixt": np.random.choice(["a", "b", "c", "d"], size=100).tolist(),
    }
)
