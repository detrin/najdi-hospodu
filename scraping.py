import requests
from bs4 import BeautifulSoup
import re
import datetime
import os
import json
import time
import random
import argparse
from tqdm import tqdm
from multiprocessing import Pool
from itertools import product
from dotenv import load_dotenv

load_dotenv()


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
    pattern = r"^\s*(?:(\d+)\s*hod)?(?:\s*(\d+)\s*min)?\s*$"
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
        0: "po",  # Monday -> po
        1: "út",  # Tuesday -> út
        2: "st",  # Wednesday -> st
        3: "čt",  # Thursday -> čt
        4: "pá",  # Friday -> pá
        5: "so",  # Saturday -> so
        6: "ne",  # Sunday -> ne
    }

    day = dt.day
    month = dt.month
    year = dt.year
    weekday = dt.weekday()
    abbreviation = day_abbreviations.get(weekday, "")
    date_str = f"{day}.{month}.{year} {abbreviation}"
    time_str = dt.strftime("%H:%M")

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "content-type": "application/x-www-form-urlencoded",
        "dnt": "1",
        "origin": "https://idos.cz",
        "priority": "u=0, i",
        "referer": "https://idos.cz/pid/spojeni/",
    }

    data = [
        ("From", from_stop),
        ("positionACPosition", ""),
        ("To", to_stop),
        ("positionACPosition", ""),
        ("AdvancedForm.Via[0]", ""),
        ("AdvancedForm.ViaHidden[0]", ""),
        ("Date", date_str),
        ("Time", time_str),
        ("IsArr", "True"),
    ]

    url = "https://idos.cz/pid/spojeni/"

    proxy_domain = os.getenv("PROXY_DOMAIN")
    proxy_port = os.getenv("PROXY_PORT")
    proxy_username = os.getenv("PROXY_USERNAME")
    proxy_password = os.getenv("PROXY_PASSWORD")

    proxy_url = f"http://{proxy_username}:{proxy_password}@{proxy_domain}:{proxy_port}"

    proxies = {
        "http": proxy_url,
        "https": proxy_url,
    }

    try:
        if proxy_domain is None:
            response = requests.post(url, headers=headers, data=data, timeout=15)
        else:
            response = requests.post(
                url, headers=headers, data=data, proxies=proxies, timeout=15
            )
        response.raise_for_status()
    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to retrieve data from {url}.") from e

    soup = BeautifulSoup(response.content, "html.parser")
    connection_head = soup.find(class_="connection-head")

    if not connection_head:
        raise ValueError("No elements found with the class 'connection-head'.")

    strong_tag = connection_head.find("strong")

    if not strong_tag:
        raise ValueError(
            "No <strong> tag found within the first 'connection-head' element."
        )

    time_str_response = strong_tag.get_text(strip=True)
    total_minutes = parse_time_to_minutes(time_str_response)
    return total_minutes


def get_total_minutes_with_retries(
    from_stop: str,
    to_stop: str,
    dt: datetime,
    max_retries: int = 3,
    retry_delay: int = 2,
) -> int:
    """
    Calculate the total travel time in minutes between two stops with retry functionality.

    Parameters:
    from_stop (str): The name of the starting stop.
    to_stop (str): The name of the destination stop.
    dt (datetime): The date and time for which the travel time is being calculated.
    max_retries (int, optional): Maximum number of retry attempts if an error occurs. Default is 3.
    retry_delay (int, optional): Delay in seconds between retry attempts. Default is 2 seconds.

    Returns:
    int: The total travel time in minutes if successful, or `None` if all attempts fail.
    """
    attempt = 0

    while attempt < max_retries:
        try:
            total_minutes = get_total_minutes(from_stop, to_stop, dt)
            return total_minutes
        except Exception as e:
            attempt += 1
            if attempt < max_retries:
                print(
                    f"Error processing pair ({from_stop}, {to_stop}): {e}. Retrying in {retry_delay} seconds... (Attempt {attempt}/{max_retries})"
                )
                time.sleep(retry_delay)
            else:
                print(
                    f"Failed to process pair ({from_stop}, {to_stop}) after {max_retries} attempts."
                )
                return None
    return None


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
    next_friday_20 = next_friday_date.replace(
        hour=target_hour, minute=0, second=0, microsecond=0
    )
    return next_friday_20


def process_pair(args):
    from_stop, to_stop, meetup_dt = args
    if from_stop == to_stop:
        return None

    total_minutes = get_total_minutes_with_retries(
        from_stop, to_stop, meetup_dt, max_retries=1
    )

    if total_minutes is not None:
        return {"from": from_stop, "to": to_stop, "total_minutes": total_minutes}
    else:
        return {"from": from_stop, "to": to_stop, "error": "Failed to retrieve data."}


def main():
    parser = argparse.ArgumentParser(description="Scraping and Correcting Script")

    parser.add_argument(
        "--stops_file",
        type=str,
        default="Prague_stops.txt",
        help="Path to the stops file.",
    )
    parser.add_argument(
        "--results",
        type=str,
        default="results.json",
        help="Path to the final results file.",
    )
    parser.add_argument(
        "--num-processes", type=int, default=5, help="Number of parallel processes."
    )
    # Adding the --num-tasks argument
    parser.add_argument(
        "--num-tasks",
        type=int,
        default=None,
        help="Number of tasks to process. If not set, all tasks will be processed.",
    )

    args = parser.parse_args()
    results_file = args.results
    stops_file = args.stops_file
    num_processes = args.num_processes
    num_tasks = args.num_tasks  # Retrieve the num_tasks value

    raw_results = []
    if os.path.exists(results_file):
        with open(results_file, "r", encoding="utf-8") as f:
            raw_results = json.load(f)

    with open(stops_file, "r", encoding="utf-8") as f:
        stops = [line.strip() for line in f if line.strip()]

    meetup_dt = get_next_meetup_time(4, 20)
    print(f"Next meetup: {meetup_dt}")

    all_pairs = list(product(stops, stops))
    unique_pairs = [pair for pair in all_pairs if pair[0] != pair[1]]
    print(f"Total unique pairs to process: {len(unique_pairs)}")

    processed_pairs_ids = {}
    correct_entries = []
    error_entries = []
    error_entries_to_process = []
    for entry in raw_results:
        processed_pairs_ids.update({(entry["from"], entry["to"]): True})
        if "error" not in entry:
            correct_entries.append(entry)
        else:
            error_entries.append(entry)
            error_entries_to_process.append((entry["from"], entry["to"], meetup_dt))

    missing_entries_to_process = []
    for entry in tqdm(unique_pairs, desc="Checking"):
        if entry not in processed_pairs_ids:
            missing_entries_to_process.append((entry[0], entry[1], meetup_dt))

    print(f"Total correct entries: {len(correct_entries)}")
    print(f"Total entries with errors to retry: {len(error_entries_to_process)}")
    print(f"Total missing entries to process: {len(missing_entries_to_process)}")

    # Combine error retries and missing entries
    args_to_process = error_entries_to_process + missing_entries_to_process

    random.shuffle(args_to_process)

    if num_tasks is not None:
        args_to_process = args_to_process[:num_tasks]
        print(f"Limiting to the first {num_tasks} tasks as specified by --num-tasks.")

    if not args_to_process:
        print("No entries to process.")
        return

    combined_results = correct_entries + error_entries
    new_results = []
    with Pool(processes=num_processes) as pool:
        for result in tqdm(
            pool.imap_unordered(process_pair, args_to_process),
            total=len(args_to_process),
            desc="Processing",
        ):
            if result is not None:
                new_results.append(result)

    # Final save after processing all tasks
    combined_results += new_results
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(combined_results, f, ensure_ascii=False, indent=4)

    failed_results = [entry for entry in new_results if "error" in entry]
    print(f"Total failed results: {len(failed_results)}")


if __name__ == "__main__":
    main()
