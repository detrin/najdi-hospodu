import requests
from bs4 import BeautifulSoup
import re
import datetime
import os
import json
import time
import argparse
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from itertools import product

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
    # Regular expression to capture hours and minutes
    pattern = r'^\s*(?:(\d+)\s*hod)?(?:\s*(\d+)\s*min)?\s*$'
    match = re.match(pattern, time_str, re.IGNORECASE)
    
    if not match:
        raise ValueError(f"Invalid time format: '{time_str}'")
    
    hours_str, minutes_str = match.groups()
    
    # Convert captured groups to integers, defaulting to 0 if not present
    hours = int(hours_str) if hours_str else 0
    minutes = int(minutes_str) if minutes_str else 0
    
    # Validate the extracted values
    if hours < 0:
        raise ValueError("Hours cannot be negative.")
    if minutes < 0:
        raise ValueError("Minutes cannot be negative.")
    if minutes >= 60:
        raise ValueError("Minutes must be less than 60.")
    
    total_minutes = hours * 60 + minutes
    return total_minutes

def get_total_minutes(from_stop: str, to_stop: str, dt: datetime.datetime) -> int:
    """
    Sends a POST request to the specified URL and parses the response to extract time in minutes.

    Args:
        headers (dict): The headers to include in the POST request.
        data (dict): The form data to include in the POST request.

    Returns:
        int: The total time in minutes extracted from the response.

    Raises:
        requests.HTTPError: If the HTTP request returned an unsuccessful status code.
        ValueError: If expected HTML elements are not found in the response.
    """
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
    
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to retrieve data from {url}.") from e

    soup = BeautifulSoup(response.content, 'html.parser')
    connection_head = soup.find(class_='connection-head')

    if not connection_head:
        print(response.text)
        raise ValueError("No elements found with the class 'connection-head'.")

    strong_tag = connection_head.find('strong')

    if not strong_tag:
        raise ValueError("No <strong> tag found within the first 'connection-head' element.")

    time_str = strong_tag.get_text(strip=True)
    total_minutes = parse_time_to_minutes(time_str)
    return total_minutes

def get_next_meetup_time(target_weekday: int, target_hour: int) -> datetime.datetime:
    """
    Returns the next meetup datetime.
    
    :param target_weekday: The target weekday (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
    :param target_hour: The target hour (0-23).
    """
    start_dt = datetime.datetime.now()

    current_weekday = start_dt.weekday()
    days_ahead = target_weekday - current_weekday

    if days_ahead == 0:
        if start_dt.time() >= datetime.time(target_hour, 0):
            days_ahead = 7  
        else:
            days_ahead = 0 
    elif days_ahead < 0:
        days_ahead += 7 

    next_friday_date = start_dt + datetime.timedelta(days=days_ahead)
    next_friday_20 = next_friday_date.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    return next_friday_20

def process_pair(args):
    from_stop, to_stop, meetup_dt = args
    if from_stop == to_stop:
        return None  # Skip if stops are the same

    max_retries = 3
    retry_delay = 10  # seconds
    attempt = 0

    while attempt < max_retries:
        try:
            total_minutes = get_total_minutes(from_stop, to_stop, meetup_dt)
            return {
                "from": from_stop,
                "to": to_stop,
                "total_minutes": total_minutes
            }
        except Exception as e:
            attempt += 1
            if attempt < max_retries:
                print(f"Error processing pair ({from_stop}, {to_stop}): {e}. Retrying in {retry_delay} seconds... (Attempt {attempt}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"Failed to process pair ({from_stop}, {to_stop}) after {max_retries} attempts.")
                return {
                    "from": from_stop,
                    "to": to_stop,
                    "error": str(e)
                }

def scrape_mode(stops_file, num_processes, output_file):
    if not os.path.exists(stops_file):
        print(f"File {stops_file} does not exist.")
        return

    # Read all stops from the file
    with open(stops_file, 'r', encoding='utf-8') as f:
        stops = [line.strip() for line in f if line.strip()]

    stops = stops[:14]  # Limit the number of stops for testing
    # Generate all unique (from, to) combinations where from != to
    all_pairs = list(product(stops, stops))
    unique_pairs = [pair for pair in all_pairs if pair[0] != pair[1]]
    print(f"Total unique pairs to process: {len(unique_pairs)}")

    # Get the next meetup time
    meetup_dt = get_next_meetup_time(4, 18)  # Assuming this function is defined
    print(f"Next meetup: {meetup_dt}")

    # Prepare arguments for the worker function
    args = [(from_stop, to_stop, meetup_dt) for from_stop, to_stop in unique_pairs]

    results = []
    with Pool(processes=num_processes) as pool:
        for result in tqdm(pool.imap_unordered(process_pair, args), total=len(args), desc="Processing"):
            if result is not None:
                results.append(result)
            if len(results) % 100 == 0:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=4)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    print(f"Results have been saved to {output_file}")

def correcting_mode(results_raw_file, results_output_file, num_processes):
    if not os.path.exists(results_raw_file):
        print(f"File {results_raw_file} does not exist.")
        return

    # Load existing raw results
    with open(results_raw_file, 'r', encoding='utf-8') as f:
        raw_results = json.load(f)

    # Identify entries with errors
    error_entries = [
        (entry["from"], entry["to"], get_next_meetup_time(4, 18))
        for entry in raw_results
        if "error" in entry
    ]

    print(f"Total entries with errors to retry: {len(error_entries)}")

    if not error_entries:
        print("No errors found. Nothing to correct.")
        return

    # Retry scraping for errored entries
    new_results = []
    with Pool(processes=num_processes) as pool:
        for result in tqdm(pool.imap_unordered(process_pair, error_entries), total=len(error_entries), desc="Correcting"):
            if result is not None:
                new_results.append(result)

    # Filter out the old errored entries
    successful_old_results = [entry for entry in raw_results if "error" not in entry]

    # Combine with new results
    combined_results = successful_old_results + new_results

    # Save to results.json
    with open(results_output_file, 'w', encoding='utf-8') as f:
        json.dump(combined_results, f, ensure_ascii=False, indent=4)

    print(f"Combined results have been saved to {results_output_file}")

def main():
    parser = argparse.ArgumentParser(description="Scraping and Correcting Script")
    parser.add_argument(
        "--task",
        type=str,
        choices=["scraping", "correcting"],
        required=True,
        help="Task to perform: 'scraping' to perform scraping, 'correcting' to correct errors."
    )
    parser.add_argument(
        "--stops_file",
        type=str,
        default="Prague_stops.txt",
        help="Path to the stops file."
    )
    parser.add_argument(
        "--results_raw",
        type=str,
        default="data/results_raw.json",
        help="Path to the raw results file."
    )
    parser.add_argument(
        "--results",
        type=str,
        default="data/results.json",
        help="Path to the final results file."
    )
    parser.add_argument(
        "--num_processes",
        type=int,
        default=5,
        help="Number of parallel processes."
    )

    args = parser.parse_args()

    if args.task == "scraping":
        scrape_mode(args.stops_file, args.num_processes, args.results_raw)
    elif args.task == "correcting":
        correcting_mode(args.results_raw, args.results, args.num_processes)
    else:
        print(f"Unknown task: {args.task}")

if __name__ == '__main__':
    main()