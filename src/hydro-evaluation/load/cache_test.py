import pandas as pd

path = 'bias_model.h5'
cache_group = 'bias_model'
configuration = 'short_range'
reference_time = '20210101T01Z'

store = pd.HDFStore(path)

df = pd.DataFrame(
    {
        "A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": pd.Categorical(["test", "train", "test", "train"]),
        "E": "foo",
    }
)
key = f'/{cache_group}/{configuration}/DT{reference_time}'

# store df
store.put(
    key = key,
    value = df,
    format = 'table',
)

# get df
if key in store:
    new_df = store[key]
    print(new_df)