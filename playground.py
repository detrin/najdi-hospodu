import polars as pl
import json

def load_time_table(results_file):
    with open(results_file, 'r', encoding='utf-8') as f:
        results = json.load(f)

    results = pl.DataFrame(results, infer_schema_length=10000)
    if "error" in results.columns:
        results = results.filter(pl.col("err").is_null()).drop("err")

    from_stops = results["from"].unique().sort().to_list()
    to_stops = results["from"].unique().sort().to_list()
    common_stops = list(set(from_stops) & set(to_stops))
    results = results.filter(results["from"].is_in(common_stops) & results["to"].is_in(common_stops))

    diagonal_pairs = []
    for stop in common_stops:
        if results.filter(pl.col("from") == stop).filter(pl.col("to") == stop).height == 0:
            diagonal_pairs.append({
                "from": stop,
                "to": stop,
                "total_minutes": 0
            })
    if len(diagonal_pairs) > 0:
        diagonal_pairs = pl.DataFrame(diagonal_pairs)
        results = pl.concat([results, diagonal_pairs])

    return results



def get_optimal_stop(time_table, method, selected_stops):
    dfs = []
    for si, stop in enumerate(selected_stops):
        df = (
            time_table
            .filter(pl.col("from") == stop)
            .drop("from")
            .with_columns(
                pl.col("to").alias("target_stop"),
                pl.col("total_minutes").alias(f"total_minutes_{si}")
            )
            .select("target_stop", f"total_minutes_{si}")
        )
        dfs.append(df)

    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.join(dfs[i], on="target_stop")

    df = df.with_columns(
        pl.max_horizontal(f"total_minutes_{si}" for si in range(len(selected_stops))).alias("worst_case_minutes"),
        pl.sum_horizontal(f"total_minutes_{si}" for si in range(len(selected_stops))).alias("total_minutes")
    )

    method = "minimize-total"
    if method == "minimize-worst-case":
        df = df.sort("worst_case_minutes")
        df_top = df.head(10)
    elif method == "minimize-total":
        df = df.sort("total_minutes")
        df_top = df.head(10)
    
    return df_top
    
results_file = "data/results.json"
time_table = load_time_table(results_file)
from_stops = time_table["from"].unique().sort().to_list()
to_stops = time_table["to"].unique().sort().to_list()
common_stops = list(set(from_stops) & set(to_stops))

selected_stops = common_stops[:3]
method = "minimize-total"
df_top = get_optimal_stop(time_table, method, selected_stops)
print(df_top)