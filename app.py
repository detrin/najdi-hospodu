import gradio as gr
import polars as pl
import json
import pandas as pd
from tqdm import tqdm

def load_time_table(results_file):
    with open(results_file, 'r', encoding='utf-8') as f:
        results = json.load(f)

    print("Loading time table file ...")
    results = pl.DataFrame(results, infer_schema_length=10000)
    if "error" in results.columns:
        results = results.filter(pl.col("err").is_null()).drop("err")

    from_stops = results["from"].unique().sort().to_list()
    to_stops = results["to"].unique().sort().to_list()  # Fixed to_stops to use "to" column
    common_stops = list(set(from_stops) & set(to_stops))
    print("Common stops:", common_stops)
    results = results.filter(results["from"].is_in(common_stops) & results["to"].is_in(common_stops))

    diagonal_pairs = []
    print("Checking for diagonal pairs ...")
    for stop in tqdm(common_stops):
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

def get_optimal_stop(time_table, method, selected_stops, show_top=20):
    dfs = []
    for si, stop in tqdm(enumerate(selected_stops), desc="Calculating optimal stops", total=len(selected_stops)):
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

    print("Joining dataframes ...")
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.join(dfs[i], on="target_stop")

    print("Calculating worst case and total minutes ...")
    df = df.with_columns(
        pl.max_horizontal(*[f"total_minutes_{si}" for si in range(len(selected_stops))]).alias("worst_case_minutes"),
        pl.sum_horizontal(*[f"total_minutes_{si}" for si in range(len(selected_stops))]).alias("total_minutes")
    )

    if method == "minimize-worst-case":
        df = df.sort("worst_case_minutes")
    elif method == "minimize-total":
        df = df.sort("total_minutes")

    return df.head(show_top)

results_file = "data/results.json"
print("Loading time table...")
TIME_TABLE = load_time_table(results_file)
from_stops = TIME_TABLE["from"].unique().sort().to_list()
to_stops = TIME_TABLE["to"].unique().sort().to_list()
ALL_STOPS = sorted(list(set(from_stops) & set(to_stops)))
SHOW_TOP = 20

with gr.Blocks() as demo:
    gr.Markdown("## Optimal Public Transport Stop Finder in Prague")
    gr.Markdown("Consider you are in Prague and you want to meet with your friends. What is the optimal stop to meet? Now you can find that with this app!")
    gr.Markdown("Time table data was scraped using IDOS API, IDOS uses PID timetable data. The arrivals are calculated based on the shortest route to the target stop from starting stops so that you all meet on Friday 24.1.2025 20:00 CET.")


    number_of_stops = gr.Slider(
        minimum=2, 
        maximum=12, 
        step=1, 
        value=3, 
        label="Number of People"
    )

    method = gr.Radio(
        choices=["Minimize worst case for each", "Minimize total time"],
        label="Optimization Method"
    )

    dropdowns = []
    for i in range(12):
        dd = gr.Dropdown(
            choices=ALL_STOPS, 
            label=f"Choose Starting Stop #{i+1}",
            visible=False  # Start hidden; we will unhide as needed
        )
        dropdowns.append(dd)

    def update_dropdowns(n):
        updates = []
        for i in range(12):
            if i < n:
                updates.append(gr.update(visible=True))
            else:
                updates.append(gr.update(visible=False))
        return updates

    number_of_stops.change(
        fn=update_dropdowns,
        inputs=number_of_stops,
        outputs=dropdowns 
    )

    search_button = gr.Button("Search")

    def search_optimal_stop(num_stops, chosen_method, *all_stops):
        # Extract selected stops based on the number of stops
        selected_stops = [stop for stop in all_stops[:num_stops] if stop]
        print("Number of stops:", num_stops)
        print("Method selected:", chosen_method)
        print("Selected stops:", selected_stops)
        
        if chosen_method == "Minimize worst case for each":
            method_key = "minimize-worst-case"
        else:
            method_key = "minimize-total"
        
        df_top = get_optimal_stop(TIME_TABLE, method_key, selected_stops, show_top=SHOW_TOP)
        df_top = df_top.with_row_index("#", offset=1)
        return df_top.to_pandas()

    results_table = gr.Dataframe(
        headers=["Target Stop", "Worst Case Minutes", "Total Minutes"],
        datatype=["str", "number", "str"],
        label="Optimal Stops"
    )

    search_button.click(
        fn=search_optimal_stop,
        inputs=[number_of_stops, method] + dropdowns,
        outputs=results_table
    )

    demo.load(
        lambda: [gr.update(visible=True) for _ in range(3)] + [gr.update(visible=False) for _ in range(9)],
        inputs=[],
        outputs=dropdowns
    )
    
    gr.Markdown("---")
    gr.Markdown("Created by [Daniel Herman](https://www.hermandaniel.com), check out the code [detrin/najdi-hospodu](https://github.com/detrin/najdi-hospodu).")


if __name__ == "__main__":
    demo.launch()