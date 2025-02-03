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


stops_geo = geo_data
stops_comb = stop_combinations
unique_stops_from_geo = list(set(stops_geo["name"].to_list()))
for_replacement = [
    "Hlavní nádraží (v ul.Opletalova)",
    "Praha hl.n.",
    "Praha Masarykovo n."
]
replace_with = [
    "Hlavní nádraží",
    "Hlavní nádraží",
    "Masarykovo nádraží"
]

stops_comb = (
    stops_comb
    .with_columns(
        pl.col("from").replace(for_replacement, replace_with).alias("from"),
        pl.col("to").replace(for_replacement, replace_with).alias("to")
    )
    .group_by("from", "to").agg(
        pl.col("total_minutes").min().alias("total_minutes")
    )
)
unique_stops_from_comb = stops_comb["from"].to_list() + stops_comb["to"].to_list()
unique_stops_from_comb = list(set(unique_stops_from_comb))
print(len(unique_stops_from_comb))
common_stops = list(set(unique_stops_from_geo) & set(unique_stops_from_comb))
print(len(common_stops))

stops_geo_common = stops_geo.filter(pl.col("name").is_in(common_stops))
stops_comb_common = stops_comb.filter(pl.col("from").is_in(common_stops) & pl.col("to").is_in(common_stops))

diagonal_elements = []
for stop in common_stops:
    diagonal_elements.append({
        "from": stop,
        "to": stop,
        "total_minutes": 0
    })
stops_comb_common = pl.concat([
    stops_comb_common,
    pl.DataFrame(diagonal_elements)
])

import geopy.distance

stops_geo_dist = (
    stops_geo_common.join(stops_geo_common, how="cross")
    .with_columns(
        pl.struct(["lat", "lon", "lat_right", "lon_right"])
        .map_elements(
            lambda x: geopy.distance.geodesic(
                (x["lat"], x["lon"]), (x["lat_right"], x["lon_right"])
            ).km,
            return_dtype=pl.Float64,
        )
        .alias("distance_in_km")
    )
    .rename({"name": "from", "name_right": "to"})
    .select(["from", "to", "distance_in_km"])
)

data = stops_geo_dist.join(stops_comb_common, on=["from", "to"], how="inner")
data = data.sort("from", "to")
print(data)

stops_geo_common.write_parquet("Prague_stops_geo.parquet")
data.write_parquet("Prague_stops_combinations.parquet")