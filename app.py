import gradio as gr
import polars as pl
import json
import math
import pandas as pd
from tqdm import tqdm
import geopy.distance


print("Loading time table file ...")
prague_stops = pl.read_csv('Prague_stops_geo.csv')
print("Calculating distances between stops ...")
stops_geo_dist = (
    prague_stops.join(prague_stops, how='cross')
    .with_columns(
        pl.struct(['lat', 'lon', 'lat_right', 'lon_right']).map_elements(
            lambda x: geopy.distance.geodesic((x['lat'], x['lon']), (x['lat_right'], x['lon_right'])).km
        ).alias('distance_in_km')
    )
    .rename({"name": "from", "name_right": "to"})
    .select(["from", "to", "distance_in_km"])
)
print(stops_geo_dist)