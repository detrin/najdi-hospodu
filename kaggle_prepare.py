import polars as pl
import json

geo_data = pl.read_csv("Prague_stops_geo.csv")
print(geo_data)

stop_combinations = None
with open("results.json", "r") as f:
    stop_combinations = json.load(f)
stop_combinations = pl.DataFrame(stop_combinations, infer_schema_length=2000000)
stop_combinations = stop_combinations.filter(pl.col("total_minutes").is_not_null())
print(stop_combinations)

geo_data.write_parquet("Prague_stops_geo.parquet")
stop_combinations.write_parquet("Prague_stops_combinations.parquet")
