import gradio as gr
import polars as pl
import json
import math
from datetime import datetime, timedelta, time
import pandas as pd
from tqdm import tqdm
import geopy.distance
import requests
import re
import os
from bs4 import BeautifulSoup
import random
from cachetools import cached, TTLCache



def get_geo_optimal_stop(method, selected_stops, show_top=20):
    global DISTANCE_TABLE
    
    dfs = []
    for si, stop in tqdm(enumerate(selected_stops), desc="Calculating optimal stops", total=len(selected_stops)):
        df = (
            DISTANCE_TABLE
            .filter(pl.col("from") == stop)
            .drop("from")
            .with_columns(
                pl.col("to").alias("target_stop"),
                pl.col("distance_in_km").alias(f"distance_in_km_{si}")
            )
            .select("target_stop", f"distance_in_km_{si}")
        )
        dfs.append(df)

    print("Joining dataframes ...")
    df = dfs[0]
    for i in range(1, len(dfs)):
        df = df.join(dfs[i], on="target_stop")

    print("Finidng optimal stops ...")
    df = df.with_columns(
        pl.max_horizontal(*[f"distance_in_km_{si}" for si in range(len(selected_stops))]).alias("worst_case_km"),
        pl.sum_horizontal(*[f"distance_in_km_{si}" for si in range(len(selected_stops))]).alias("total_km")
    )

    if method == "minimize-worst-case":
        df = df.sort("worst_case_km")
    elif method == "minimize-total":
        df = df.sort("total_km")

    return df.head(show_top)

print("Loading time table file ...")
prague_stops = pl.read_csv('Prague_stops_geo.csv')
print("Calculating distances between stops ...")
# stops_geo_dist = (
#     prague_stops.join(prague_stops, how='cross')
#     .with_columns(
#         pl.struct(['lat', 'lon', 'lat_right', 'lon_right']).map_elements(
#             lambda x: geopy.distance.geodesic((x['lat'], x['lon']), (x['lat_right'], x['lon_right'])).km
#         ).alias('distance_in_km')
#     )
#     .rename({"name": "from", "name_right": "to"})
#     .select(["from", "to", "distance_in_km"])
# )
stops_geo_dist = pl.read_csv("Prague_stops_geo_dist.csv")
DISTANCE_TABLE = stops_geo_dist
from_stops = DISTANCE_TABLE["from"].unique().sort().to_list()
to_stops = DISTANCE_TABLE["to"].unique().sort().to_list()
ALL_STOPS = sorted(list(set(from_stops) & set(to_stops)))
SHOW_TOP = 20


def validate_date(date_str):
    """
    Validates that the date string is in DD/MM/YYYY format and represents a valid date.
    """
    try:
        datetime.strptime(date_str, "%d/%m/%Y")
        return True
    except ValueError:
        return False

def validate_time(time_str):
    """
    Validates that the time string is in HH:MM format and represents a valid time.
    """
    try:
        datetime.strptime(time_str, "%H:%M")
        return True
    except ValueError:
        return False
    
def validate_date_time(date_str, time_str):
    """
    Validates that:
    1. The date string is in DD/MM/YYYY format and represents a valid date.
    2. The time string is in HH:MM format and represents a valid time.
    3. The combined datetime is in the future and not more than 3 months ahead.
    
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    try:
        event_datetime = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
    except ValueError:
        return False, "Invalid date or time format. Please ensure date is DD/MM/YYYY and time is HH:MM."

    now = datetime.now()
    three_months_later = now + timedelta(days=90)  # Approximation of 3 months

    if event_datetime <= now:
        return False, "The selected date and time must be in the future."
    if event_datetime > three_months_later:
        return False, "The selected date and time must not be more than 3 months in the future."

    return True, ""


def get_next_meetup_time(target_weekday: int, target_hour: int) -> datetime:
    """
    Returns the next meetup datetime.
    
    :param target_weekday: The target weekday (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
    :param target_hour: The target hour (0-23).
    """
    start_dt = datetime.now()

    current_weekday = start_dt.weekday()
    days_ahead = target_weekday - current_weekday

    if days_ahead == 0:
        if start_dt.time() >= time(target_hour, 0):
            days_ahead = 7  
        else:
            days_ahead = 0 
    elif days_ahead < 0:
        days_ahead += 7 

    next_dt = start_dt + timedelta(days=days_ahead)
    next_dt = next_dt.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    return next_dt


def parse_time_to_minutes(time_str: str) -> int:
    """
    Parses a time string and returns the total number of minutes as an integer.

    Supported formats:
        - "X hod Y min" (e.g., "1 hod 1 min")
        - "X hod" (e.g., "2 hod")
        - "Y min" (e.g., "20 min")
    
    Forbidden inputs:
        - Negative minutes or hours (e.g., "-1 min")
        - Minutes equal to or exceeding 60 (e.g., "61 min")
        - Incorrect formats

    Args:
        time_str (str): The time string to parse.

    Returns:
        int: Total number of minutes.

    Raises:
        ValueError: If the input format is invalid or contains forbidden values.
    """
    pattern = r'^\s*(?:(\d+)\s*hod)?(?:\s*(\d+)\s*min)?\s*$'
    match = re.match(pattern, time_str, re.IGNORECASE)
    
    if not match:
        raise ValueError(f"Invalid time format: '{time_str}'")
    
    hours_str, minutes_str = match.groups()
    
    hours = int(hours_str) if hours_str else 0
    minutes = int(minutes_str) if minutes_str else 0

    if hours < 0:
        raise ValueError("Hours cannot be negative.")
    if minutes < 0:
        raise ValueError("Minutes cannot be negative.")
    if minutes >= 60:
        raise ValueError("Minutes must be less than 60.")
    
    total_minutes = hours * 60 + minutes
    return total_minutes

@cached(cache=TTLCache(maxsize=1024, ttl=600))
def get_total_minutes(from_stop: str, to_stop: str, dt: datetime) -> int:
    """
    Sends a POST request to the specified URL using Webshare's rotating proxy and parses the response to extract time in minutes.

    Args:
        from_stop (str): The departure stop.
        to_stop (str): The arrival stop.
        dt (datetime.datetime): The date and time for the query.

    Returns:
        int: The total time in minutes extracted from the response.

    Raises:
        requests.HTTPError: If the HTTP request returned an unsuccessful status code.
        ValueError: If expected HTML elements are not found in the response.
    """
    
    if from_stop == to_stop:
        return 0
    
    day_abbreviations = {
        0: 'po',  # Monday -> po
        1: 'út',  # Tuesday -> út
        2: 'st',  # Wednesday -> st
        3: 'čt',  # Thursday -> čt
        4: 'pá',  # Friday -> pá
        5: 'so',  # Saturday -> so
        6: 'ne'   # Sunday -> ne
    }
    
    day = dt.day
    month = dt.month
    year = dt.year
    weekday = dt.weekday() 
    abbreviation = day_abbreviations.get(weekday, '')
    date_str = f"{day}.{month}.{year} {abbreviation}"
    time_str = dt.strftime('%H:%M')
    
    
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'dnt': '1',
        'origin': 'https://idos.cz',
        'priority': 'u=0, i',
        'referer': 'https://idos.cz/pid/spojeni/',
    }

    data = [
        ('From', from_stop),
        ('positionACPosition', ''),
        ('To', to_stop),
        ('positionACPosition', ''),
        ('AdvancedForm.Via[0]', ''),
        ('AdvancedForm.ViaHidden[0]', ''),
        ('Date', date_str),
        ('Time', time_str),
        ('IsArr', 'True'),
    ]

    url = 'https://idos.cz/pid/spojeni/'
    
    proxy_domain = os.getenv("PROXY_DOMAIN")
    proxy_port = os.getenv("PROXY_PORT")
    proxy_username = os.getenv("PROXY_USERNAME")
    proxy_password = os.getenv("PROXY_PASSWORD")

    # Construct the proxy URL with authentication
    proxy_url = f"http://{proxy_username}:{proxy_password}@{proxy_domain}:{proxy_port}"
    
    proxies = {
        'http': proxy_url,
        'https': proxy_url,
    }

    try:
        if proxy_domain is None:
            response = requests.post(url, headers=headers, data=data, timeout=30)
        else:
            response = requests.post(url, headers=headers, data=data, proxies=proxies, timeout=30)
        response.raise_for_status() 
    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to retrieve data from {url}.") from e

    soup = BeautifulSoup(response.content, 'html.parser')
    connection_head = soup.find(class_='connection-head')

    if not connection_head:
        raise ValueError("No elements found with the class 'connection-head'.")

    strong_tag = connection_head.find('strong')

    if not strong_tag:
        raise ValueError("No <strong> tag found within the first 'connection-head' element.")

    time_str_response = strong_tag.get_text(strip=True)
    total_minutes = parse_time_to_minutes(time_str_response)
    return total_minutes

def get_time_optimal_stop(method, selected_stops, target_stops, event_datetime, show_top=20):
    rows = []
    for target_stop in tqdm(target_stops):
        row = {"target_stop": target_stop}
        for si, from_stop in enumerate(selected_stops):
            try:
                total_minutes = get_total_minutes(from_stop, target_stop, event_datetime)
                row[f"total_minutes_{si}"] = total_minutes
            except Exception as e:
                print(f"Error processing pair ({from_stop}, {target_stop}): {e}")
                row[f"total_minutes_{si}"] = None
        rows.append(row)
        
    df_times = (
        pl.DataFrame(rows) 
            .with_columns(
                pl.max_horizontal(*[f"total_minutes_{si}" for si in range(len(selected_stops))]).alias("worst_case_minutes"),
                pl.sum_horizontal(*[f"total_minutes_{si}" for si in range(len(selected_stops))]).alias("total_minutes")
            )
    )

    if method == "minimize-worst-case":
        df_times = df_times.sort("worst_case_minutes")
    elif method == "minimize-total":
        df_times = df_times.sort("total_minutes")
        
    df_times.rename({"worst_case_minutes": "Worst Case Minutes", "total_minutes": "Total Minutes"})
    for si in range(len(selected_stops)):
        df_times = df_times.rename(f"total_minutes_{si}", f"t{selected_stops[si]} mins")
        
    return df_times.head(show_top)

with gr.Blocks() as demo:
    gr.Markdown("## Optimal Public Transport Stop Finder in Prague")
    gr.Markdown("""
    Consider you are in Prague and you want to meet with your friends. What is the optimal stop to meet? Now you can find that with this app!
    
    Time table data was scraped using IDOS API, IDOS uses PID timetable data. The arrivals are calculated based on the shortest route to the target stop from starting stops so that you all meet on Friday 24.1.2025 20:00 CET.
    """)

    # Slider to select the number of people
    number_of_stops = gr.Slider(
        minimum=2, 
        maximum=12, 
        step=1, 
        value=3, 
        label="Number of People"
    )

    # Radio buttons to select the optimization method
    method = gr.Radio(
        choices=["Minimize worst case for each", "Minimize total time"],
        value="Minimize worst case for each",
        label="Optimization Method"
    )

    next_dt = get_next_meetup_time(4, 20)  # Friday 20:00
    next_date = next_dt.strftime("%d/%m/%Y")
    next_time = next_dt.strftime("%H:%M")
    # Date input in DD/MM/YYYY format
    date_input = gr.Textbox(
        label="Date (DD/MM/YYYY)",
        placeholder=f"e.g., {next_date}",
        value=next_date
    )

    # Time input in HH:MM format
    time_input = gr.Textbox(
        label="Time (HH:MM)",
        placeholder=f"e.g., {next_time}",
        value=next_time
    )

    # Dropdowns for selecting starting stops, initially hidden
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

    # Update the visibility of dropdowns based on the number of stops selected
    number_of_stops.change(
        fn=update_dropdowns,
        inputs=number_of_stops,
        outputs=dropdowns 
    )

    # Search button to trigger the optimization
    search_button = gr.Button("Search")

    def search_optimal_stop(num_stops, chosen_method, date_str, time_str, *all_stops):
        # Validate Date
        is_valid, error_message = validate_date_time(date_str, time_str)
        if not is_valid:
            raise gr.Error(error_message)
        
        # Extract selected stops based on the number of stops
        selected_stops = [stop for stop in all_stops[:num_stops] if stop]
        print("Number of stops:", num_stops)
        print("Method selected:", chosen_method)
        print("Selected stops:", selected_stops)
        print("Selected date:", date_str)
        print("Selected time:", time_str)
        
        if chosen_method == "Minimize worst case for each":
            method = "minimize-worst-case"
        else:
            method = "minimize-total"
        
        # Here, you can modify how date_str and time_str are used in your logic
        # For example, you might want to convert them to datetime objects
        try:
            # Optionally parse date and time to datetime objects
            event_datetime = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M")
            print("Event DateTime:", event_datetime)
        except ValueError as e:
            # This should not happen due to prior validation, but added for safety
            raise gr.Error(f"Error parsing date and time: {e}")
        
        df_top = get_geo_optimal_stop(method, selected_stops, show_top=SHOW_TOP)
        target_stops = df_top["target_stop"].to_list()
        df_times = get_time_optimal_stop(method, selected_stops, target_stops, event_datetime, show_top=SHOW_TOP)
        df_times = df_times.with_row_index("#", offset=1)
        
        return df_times

    # Dataframe to display the results
    results_table = gr.Dataframe(
        headers=["Target Stop", "Worst Case Minutes", "Total Minutes"],
        datatype=["str", "number", "str"],
        label="Optimal Stops"
    )

    # Configure the search button to call the callback with the new inputs
    search_button.click(
        fn=search_optimal_stop,
        inputs=[number_of_stops, method, date_input, time_input] + dropdowns,
        outputs=results_table
    )

    # On load, display the first 3 dropdowns and hide the rest
    demo.load(
        lambda: [gr.update(visible=True) for _ in range(3)] + [gr.update(visible=False) for _ in range(9)],
        inputs=[],
        outputs=dropdowns
    )
    
    gr.Markdown("---")
    gr.Markdown("""
    Created by [Daniel Herman](https://www.hermandaniel.com), check out the code [detrin/pub-finder](https://github.com/detrin/pub-finder).
    """)


if __name__ == "__main__":
    demo.launch()