import duckdb
cursor = duckdb.connect()
print(cursor.execute("""
SELECT * FROM read_parquet('parquet/*.parquet') WHERE reference_time = '2022-10-15T00:00:00' AND nwm_feature_id = 17003262;
""").fetchall()) 