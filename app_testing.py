import gradio as gr

# Example list of bus stops
ALL_BUS_STOPS = [f"Stop no. {n}" for n in range(1, 1000)]

with gr.Blocks() as demo:
    gr.Markdown("## Dynamic Bus Stop Selection")

    # 1) Number of stops (2..12)
    number_of_stops = gr.Slider(
        minimum=2, 
        maximum=12, 
        step=1, 
        value=4, 
        label="Number of stops"
    )

    # 2) Optimization method
    method = gr.Radio(
        choices=["Minimize worst case for each", "Minimize total time"],
        label="Optimization Method"
    )

    # 3) Pre-create the maximum possible dropdowns (12).
    #    We'll just toggle visibility for those we don't need.
    dropdowns = []
    for i in range(12):
        dd = gr.Dropdown(
            choices=ALL_BUS_STOPS, 
            label=f"Choose bus stop #{i+1}",
            visible=False  # Start hidden; we will unhide as needed
        )
        dropdowns.append(dd)

    # Whenever "number_of_stops" changes, show the first N dropdowns, hide the rest.
    def update_dropdowns(n):
        updates = []
        for i in range(12):
            # Show if i < n, else hide
            if i < n:
                updates.append(gr.update(visible=True))
            else:
                updates.append(gr.update(visible=False))
        return updates

    number_of_stops.change(
        fn=update_dropdowns,
        inputs=number_of_stops,
        outputs=dropdowns  # returns a list of 12 .update() dictionaries
    )

    # 4) Button to gather the inputs
    search_button = gr.Button("Search stop")

    # 5) Final function that prints out the parameters
    def dummy_search(num_stops, chosen_method, *all_stops):
        # "all_stops" will always contain 12 entries, but only the
        # first "num_stops" are currently visible.
        selected = all_stops[:num_stops]
        print("Number of stops:", num_stops)
        print("Method selected:", chosen_method)
        print("Selected stops:", selected)
        return f"Stops selected: {', '.join(selected)}"

    # Feed all dropdowns as part of the inputs -> we can index them
    # to only read the first N in dummy_search
    search_button.click(
        fn=dummy_search,
        inputs=[number_of_stops, method] + dropdowns,
        outputs=gr.Textbox(label="Results")
    )

demo.launch()