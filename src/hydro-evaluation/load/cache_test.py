import pandas as pd

# set some variables
path = 'bias_model.h5'
cache_group = 'bias_model'
configuration = 'short_range'
reference_time = '20210101T01Z'
key = f'/{cache_group}/{configuration}/DT{reference_time}'

# create cache store
store = pd.HDFStore(path)

# create a test dataframe
df = pd.DataFrame(
    {
        "A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": pd.Categorical(["test", "train", "test", "train"]),
        "E": "foo",
    }
)

# store df to store
store.put(
    key = key,
    value = df,
    format = 'table',
)

# get df from store
if key in store:
    new_df = store[key]
    print(new_df)