from __future__ import annotations
import pandas as pd
import numpy as np
import requests
from pprint import pprint
from math import ceil
import copy
import random
from datetime import datetime, timedelta, date
import time
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import ast
import json
import ijson
import sqlite3
from tqdm import tqdm
import os
import ast
import csv
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Iterable, Sequence

pd.set_option('display.float_format', '{:.0f}'.format)

# CONSTANTS

TimeoutType = Union[float, int, Tuple[float, float]]  # seconds or (connect, read)

RETENTION_FIELDS_DAILY = [
    "est_retention_d1",
    "est_retention_d2",
    "est_retention_d3",
    "est_retention_d4",
    "est_retention_d5",
    "est_retention_d6",
    "est_retention_d7",
    "est_retention_d14",
    "est_retention_d30",
    "est_retention_d60",
    "est_retention_d90",
    "est_retention_d180",
    "est_retention_d365",
]

# FUNCTIONS

def _to_int_or_none(x) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None
    try:
        return int(float(s))  # handles "1", "1.0"
    except Exception:
        return None

def chunk_list(lst, chunk_size):
    """
    Splits a list into chunks of specified size.
    
    Parameters:
        lst (list): The list to split.
        chunk_size (int): The maximum size of each chunk.
        
    Returns:
        generator: A generator that yields chunks of the list.
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def parse_string_to_list(input_string):
    """
    Converts a string representation of a list into an actual Python list.

    Args:
        input_string (str): The input string representing a list, e.g., 
                            "['US', 'AU', 'CA']".

    Returns:
        list: A Python list parsed from the input string.
    """
    # Use eval to safely parse the string into a list
    try:
        result = eval(input_string)
        if isinstance(result, list):
            return result
        else:
            raise ValueError("Input string does not represent a valid list.")
    except Exception as e:
        raise ValueError(f"Failed to parse input string: {e}")
    
def string_to_timestamp(date_string):
    """
    Converts an ISO 8601 date string into a Unix timestamp.
    
    Args:
        date_string (str): The date string in ISO 8601 format, e.g., "2010-05-29T00:00:00Z".
    
    Returns:
        int: The Unix timestamp (seconds since epoch).
    """
    # Parse the date string to a datetime object
    dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")
    
    # Convert to a timestamp
    return int(dt.timestamp())

def string_to_timestamp_v2(date_string):
    """
    Converts a date string in 'YYYY/MM/DD' format into a Unix timestamp.
    
    Args:
        date_string (str): The date string in 'YYYY/MM/DD' format, e.g., "2013/02/25".
    
    Returns:
        int: The Unix timestamp (seconds since epoch).
    """
    # Parse the date string to a datetime object
    dt = datetime.strptime(date_string, "%Y/%m/%d")
    
    # Convert to a timestamp
    return int(dt.timestamp())

def timestamp_to_string(timestamp):
    """
    Converts a Unix timestamp into a date string in 'YYYY/MM/DD' format.
    
    Args:
        timestamp (int): The Unix timestamp to convert.
    
    Returns:
        str: The formatted date string, e.g., "2013/02/25".
    """
    # Convert the timestamp to a datetime object
    dt = datetime.fromtimestamp(timestamp)
    
    # Format the datetime object as a string
    date_string = dt.strftime("%Y/%m/%d")
    
    return date_string

def add_days_to_date(date_str, days_to_add):
    """
    Adds a certain number of days to a given date string.

    Args:
        date_str (str): The input date string in the format "YYYY-MM-DD".
        days_to_add (int): The number of days to add.

    Returns:
        str: The resulting date string in the format "YYYY-MM-DD".
    """
    # Parse the input date string to a datetime object
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    # Add the specified number of days
    new_date_obj = date_obj + timedelta(days=days_to_add)
    # Convert back to a string and return
    return new_date_obj.strftime("%Y-%m-%d")

def _get_with_retry(
    url: str,
    *,
    params: Dict[str, Any],
    timeout: TimeoutType = (10, 60),
    max_retries: int = 6,
    base_sleep: float = 1.5,
    max_sleep: float = 60,
    progress_logger: Optional[Callable[[str], None]] = None,
) -> requests.Response:
    """
    Retry on connection resets/timeouts and on 429/5xx with exponential backoff + jitter.
    Supports timeout as a single number OR (connect_timeout, read_timeout).
    """
    attempt = 0
    last_exc: Optional[Exception] = None

    while True:
        attempt += 1
        try:
            resp = requests.get(url, params=params, timeout=timeout)

            # Retryable HTTP statuses
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    return resp

                sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1))) + random.random()

                # honor Retry-After if provided
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = max(float(ra), sleep_s)
                    except Exception:
                        pass

                if progress_logger is not None:
                    progress_logger(
                        f"HTTP {resp.status_code} from Sensor Tower on attempt {attempt}/{max_retries}; "
                        f"retrying in {sleep_s:.1f}s"
                    )
                time.sleep(sleep_s)
                continue

            return resp

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ) as e:
            last_exc = e
            if attempt >= max_retries:
                raise
            sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1))) + random.random()
            if progress_logger is not None:
                progress_logger(
                    f"Network error on attempt {attempt}/{max_retries}: {type(e).__name__}: {e}. "
                    f"Retrying in {sleep_s:.1f}s"
                )
            time.sleep(sleep_s)

def get_unified_app_data(api_key, base_url, app_ids):
    """
    Fetch unified app data from the SensorTower API in batches of 100 IDs per request.
    
    Parameters:
        api_key (str): Your SensorTower API key.
        base_url (str): The base URL for the API.
        app_ids (list[str]): List of unified app IDs to fetch.
        
    Returns:
        list[dict]: Combined response data from all requests.
    """
    
    # API endpoint
    endpoint = "/v1/unified/apps"
    url = f"{base_url}{endpoint}"
    
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    all_responses = []
    chunk_size = 100
    count = 1
    
    # Split the list of app_ids into chunks of 100
    for app_id_chunk in chunk_list(app_ids, chunk_size):
        params = {
            'app_id_type': 'unified',
            'app_ids': ','.join(app_id_chunk)
        }
        
        print('making call for chunk number {}'.format(count))
        
        # Make the API request for this chunk
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            print('call successful!')
            all_responses.extend(response.json().get('apps', []))
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
        
        count += 1
    
    return all_responses

def get_required_tags(api_key, base_url, app_ids):


    # API endpoint
    endpoint = "/v1/app_tag/tags_for_apps"
    url = f"{base_url}{endpoint}"

    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    
    all_responses = []
    chunk_size = 100
    count = 1
    
    # Split the list of app_ids into chunks of 100
    for app_id_chunk in chunk_list(app_ids, chunk_size):
        # params = {
        #     'app_ids': 'eu.nordeus.topeleven.android',
        #     'field_categories[]':'gaming',  
        #     'field_categories[]':'release_info',  
        # }

        # params = {
        #     'app_ids': 'eu.nordeus.topeleven.android',
        #     'field_categories[]':['gaming', 'release_info']  
        # }

        params = {
            'app_ids': ','.join(app_id_chunk),
            'fields[]':['Game Class', 
                        'Game Genre', 
                        'Game Sub-genre',
                        'Game Art Style',
                        'Game Camera POV',
                        'Game Setting',
                        'Game Theme',
                        'Game Product Model',
                        'IP: Corporate Parent',
                        'IP: IP Operator',
                        'IP: Media Type',
                        'IP: Licensed IP',
                        'Earliest Release Date',
                        'Release Date (WW)',
                        'Release Date (US)',
                        'Release Date (JP)',
                        'Release Date (CN)'
                       ]  
        }
        
        print('making call for chunk number {}'.format(count))
        
        # Make the API request for this chunk
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            print('call successful!')
            all_responses.extend(response.json().get('data', []))
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
        
        count += 1
    
    return all_responses

def find_row_cell_value(row_name, tag_list):
    try:
        return list(filter(lambda x: x['name'] == row_name, tag_list))[0]['exact_value']
    except:
        return None
    
def get_tags_table(tags_data):
    
    tags_table = []

    for raw_row in tags_data:

        current_tag_list = raw_row['tags']

        new_row = dict()
        new_row['canonical_app_id'] = raw_row['app_id']
        new_row['game_class'] = find_row_cell_value('Game Class', current_tag_list)
        new_row['game_genre'] = find_row_cell_value('Game Genre', current_tag_list)
        new_row['game_subgenre'] = find_row_cell_value('Game Sub-genre', current_tag_list)
        new_row['game_art_style'] = find_row_cell_value('Game Art Style', current_tag_list)
        new_row['game_camera_pov'] = find_row_cell_value('Game Camera POV', current_tag_list)
        new_row['game_setting'] = find_row_cell_value('Game Setting', current_tag_list)
        new_row['game_theme'] = find_row_cell_value('Game Theme', current_tag_list)
        new_row['game_product_model'] = find_row_cell_value('Game Product Model', current_tag_list)
        new_row['game_ip_corporate_parent'] = find_row_cell_value('IP: Corporate Parent', current_tag_list)
        new_row['game_ip_operator'] = find_row_cell_value('IP: IP Operator', current_tag_list)
        new_row['game_ip_media_type'] = find_row_cell_value('IP: Media Type', current_tag_list)
        new_row['game_licensed_ip'] = find_row_cell_value('IP: Licensed IP', current_tag_list)
        new_row['game_earliest_release_date'] = find_row_cell_value('Earliest Release Date', current_tag_list)
        new_row['game_release_date_ww'] = find_row_cell_value('Release Date (WW)', current_tag_list)
        new_row['game_release_date_us'] = find_row_cell_value('Release Date (US)', current_tag_list)
        new_row['game_release_date_jp'] = find_row_cell_value('Release Date (JP)', current_tag_list)
        new_row['game_release_date_cn'] = find_row_cell_value('Release Date (CN)', current_tag_list) 
        tags_table.append(new_row)
    
    return tags_table

def extract_app_ids(list_of_dict):
    return list(map(lambda d: d['app_id'], list_of_dict))

def get_app_id_lists(unified_app_data):
#     also known as canonical data, cuz it also contains canonical id thingy
    itunes_app_id_list = []
    android_app_id_list = []
    
    for app in unified_app_data:
        current_app_itunes_app_ids = app['itunes_apps']
        current_app_android_app_ids = app['android_apps']
        itunes_app_id_list.extend(extract_app_ids(current_app_itunes_app_ids))
        android_app_id_list.extend(extract_app_ids(current_app_android_app_ids))
    
#     ios app id list needs to be adjusted (but not for android app id list)
    itunes_app_id_list_adjusted = list(map(lambda x: str(x),itunes_app_id_list))
    
    result_list = []
    
    for app_id in itunes_app_id_list_adjusted:
        result_list.append(
            {
                'os':'ios',
                'app_id':app_id
            }
        
        )

    for app_id in android_app_id_list:
        result_list.append(
            {
                'os':'android',
                'app_id':app_id
            }
        
        )
    
    return result_list

def get_local_app_info(api_key, base_url, os, app_ids):
    
    # API endpoint
    endpoint = "/v1/{}/apps".format(os)
    url = f"{base_url}{endpoint}"
    
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    all_responses = []
    chunk_size = 100
    count = 1
    
    # Split the list of app_ids into chunks of 100
    for app_id_chunk in chunk_list(app_ids, chunk_size):
        params = {
            'app_ids': ','.join(app_id_chunk)
        }
        
        print('making call for chunk number {}'.format(count))
        
        # Make the API request for this chunk
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            print('call successful!')
            all_responses.extend(response.json().get('apps', []))
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
        
        count += 1
    
    return all_responses

# A new get_local_app_info version, optimized for SQL-friendly data files
def get_local_app_info_iter(api_key, base_url, os, app_ids, *, chunk_size=100, sleep_s=0.15, timeout_s=60, max_retries=6):
    """
    Yields app dicts from SensorTower API, chunk by chunk.
    """
    endpoint = f"/v1/{os}/apps"
    url = f"{base_url}{endpoint}"

    headers = {"Authorization": f"Bearer {api_key}"}
    total_chunks = (len(app_ids) + chunk_size - 1) // chunk_size

    for chunk_idx, app_id_chunk in enumerate(chunk_list(app_ids, chunk_size), start=1):
        params = {"app_ids": ",".join(str(x) for x in app_id_chunk)}

        attempt = 0
        while True:
            attempt += 1
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=timeout_s)
            except requests.RequestException as e:
                if attempt >= max_retries:
                    print(f"\n[{os}] chunk {chunk_idx}/{total_chunks} network error (giving up): {e}")
                    break
                backoff = min(60, 2 ** (attempt - 1))
                print(f"\n[{os}] chunk {chunk_idx}/{total_chunks} network error: {e} | retry in {backoff}s")
                time.sleep(backoff)
                continue

            if resp.status_code == 200:
                apps = resp.json().get("apps", [])
                for app in apps:
                    yield app
                break

            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    print(f"\n[{os}] chunk {chunk_idx}/{total_chunks} HTTP {resp.status_code} (giving up): {resp.text[:200]}")
                    break
                retry_after = resp.headers.get("Retry-After")
                backoff = int(retry_after) if (retry_after and retry_after.isdigit()) else min(60, 2 ** (attempt - 1))
                print(f"\n[{os}] chunk {chunk_idx}/{total_chunks} HTTP {resp.status_code} | retry in {backoff}s")
                time.sleep(backoff)
                continue

            print(f"\n[{os}] chunk {chunk_idx}/{total_chunks} failed HTTP {resp.status_code}: {resp.text[:200]}")
            break

        time.sleep(sleep_s)

def get_local_app_info_from_game_full_info_table(api_key, base_url, df_game_full_info):
    
    print("Extracting app list from game table...")
    list_of_dicts_game_app_list = []

    for index,row in df_game_full_info.iterrows():
        list_of_dicts_game_app_list.append(
            {
                'unified_app_id': row['unified_app_id'],
                'itunes_apps': ast.literal_eval(str(row['itunes_apps'])),
                'android_apps': ast.literal_eval(str(row['android_apps'])),
                'itunes_publisher_ids': ast.literal_eval(str(row['itunes_publisher_ids'])),
                'android_publisher_ids': ast.literal_eval(str(row['android_publisher_ids'])),
            }
        )

    all_app_ids = get_app_id_lists(list_of_dicts_game_app_list)

    ios_app_id_list = list(map(lambda x: x['app_id'], list(filter(lambda x: x['os'] == 'ios', all_app_ids))))
    android_app_id_list = list(map(lambda x: x['app_id'], list(filter(lambda x: x['os'] == 'android', all_app_ids))))

    print("Getting iOS apps info...")
    ios_app_info_list = get_local_app_info(api_key, base_url, "ios", ios_app_id_list)

    print("Getting Android apps info...")
    android_app_info_list = get_local_app_info(api_key, base_url, "android", android_app_id_list)

    df_ios_app_info_list = pd.DataFrame(ios_app_info_list)
    df_android_app_info_list = pd.DataFrame(android_app_info_list)

    df_all_app_info_list = pd.concat(
        [df_ios_app_info_list, df_android_app_info_list],
        ignore_index=True
    )

    return df_all_app_info_list

# A new get_local_app_info_from_game_full_info_table version, optimized for SQL-friendly data files
def get_local_app_info_from_game_full_info_table_ndjson_version(
    api_key, base_url, df_game_full_info, timestamp, out_dir="data/base"
):
    print("Extracting app list from game table...")

    ios_ids = []
    android_ids = []

    for _, row in df_game_full_info.iterrows():
        itunes_apps = ast.literal_eval(str(row.get("itunes_apps")))
        android_apps = ast.literal_eval(str(row.get("android_apps")))

        # iOS: numeric app_id
        if isinstance(itunes_apps, list):
            for a in itunes_apps:
                if isinstance(a, dict) and "app_id" in a and a["app_id"] is not None:
                    try:
                        ios_ids.append(int(a["app_id"]))
                    except Exception:
                        # just skip weird ones
                        pass

        # Android: package name (string) OR sometimes numeric -> keep as string always
        if isinstance(android_apps, list):
            for a in android_apps:
                if isinstance(a, dict) and "app_id" in a and a["app_id"] is not None:
                    android_ids.append(str(a["app_id"]).strip())

    # de-dup while preserving order
    def dedup(seq):
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    ios_ids = dedup(ios_ids)
    android_ids = dedup(android_ids)

    print(f"iOS app_ids: {len(ios_ids):,}")
    print(f"Android app_ids: {len(android_ids):,}")

    out_path = f"{out_dir}/st_api_app_full_info_{timestamp}.ndjson"

    total_target = len(ios_ids) + len(android_ids)
    written = 0
    start = time.time()
    last_print = 0.0

    def progress(os_name):
        nonlocal last_print
        now = time.time()
        if now - last_print < 0.2:
            return
        elapsed = now - start
        speed = written / elapsed if elapsed > 0 else 0.0
        pct = (written / total_target * 100.0) if total_target > 0 else 100.0
        print(f"\r{pct:6.2f}%  written={written:,}/{total_target:,}  {speed:,.1f} apps/s  (now: {os_name})", end="")
        last_print = now

    with open(out_path, "w", encoding="utf-8", newline="") as f_out:
        print("Getting iOS apps info (writing NDJSON)...")
        for app in get_local_app_info_iter(api_key, base_url, "ios", ios_ids):
            f_out.write(json.dumps(app, ensure_ascii=False) + "\n")
            written += 1
            progress("ios")

        print("\nGetting Android apps info (writing NDJSON)...")
        for app in get_local_app_info_iter(api_key, base_url, "android", android_ids):
            f_out.write(json.dumps(app, ensure_ascii=False) + "\n")
            written += 1
            progress("android")

    print()
    print(f"Done. Wrote: {out_path}")
    print(f"Apps written: {written:,}")

    return out_path

def get_unified_id(local_app_id):
    return list(filter(lambda x: x['app_id'] == local_app_id, local_id_to_unified_id_mapping))[0]['unified_app_id']

def get_apps_performance(api_key, base_url, os, app_ids, country_code, granularity, max_periods, start_date, end_date):
    
    def split_date_range(start_date, end_date, granularity, max_periods):
        # max_periods = max number of periods in a range (the periods can be arbitrary - daily, weekly, monthly, quarterly, etc.)
        current_start = datetime.strptime(start_date, '%Y-%m-%d')
        final_end = datetime.strptime(end_date, '%Y-%m-%d')
        date_ranges = []
        
        day_count_per_granularity = {
            'daily': 1,
            'weekly': 7,
            'monthly': 30,
            'quarterly': 91
        }

        while current_start < final_end:
            
            # Calculate the end date for the current range
            current_end = current_start + timedelta(days=max_periods * day_count_per_granularity[granularity])
            # Ensure current_end does not exceed final_end
            current_end = min(current_end, final_end)
            
            if granularity == 'daily':
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = current_end + timedelta(days=1)
            elif granularity == 'weekly':
                current_end = current_end + timedelta(days=(6 - current_end.weekday()))
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = current_end + timedelta(days=1)
            elif granularity == 'monthly':
                current_end = (current_end.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = (current_end + timedelta(days=1)).replace(day=1)
            elif granularity == 'quarterly':
                # Move current_end to the last day of the quarter
                quarter = (current_end.month - 1) // 3 + 1  # Calculate current quarter (1, 2, 3, or 4)
                next_quarter_start_month = quarter * 3 + 1  # Month of the next quarter's first day
                if next_quarter_start_month > 12:
                    next_quarter_start = current_end.replace(year=current_end.year + 1, month=1, day=1)
                else:
                    next_quarter_start = current_end.replace(month=next_quarter_start_month, day=1)
                current_end = next_quarter_start - timedelta(days=1)  # Move to the last day of the current quarter
                date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))
                current_start = (current_end + timedelta(days=1)).replace(day=1)
#                 print('current_start: {}'.format(current_start))

        return date_ranges

    
    # API endpoint   
    endpoint = "/v1/{}/sales_report_estimates".format(os)
    url = f"{base_url}{endpoint}"
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    date_ranges = split_date_range(start_date, end_date, granularity, max_periods)
    print("Date ranges: {}".format(date_ranges))
    
    all_results = []
    chunk_size = 100

    for start, end in date_ranges:
        
        print("Requesting Data for Range:", start, "to", end)
        count = 1
        
        # Split the list of app_ids into chunks of 100
        for app_id_chunk in chunk_list(app_ids, chunk_size):

            params = {
                'os': os,
                'app_ids': ','.join(app_id_chunk),
                'countries': country_code,
                'date_granularity': granularity,
                'start_date': start,
                'end_date': end,
            }

            print('making call for chunk number {}'.format(count))

            request = requests.Request("GET", url, headers=headers, params=params)
            prepared_request = request.prepare()

            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                print(f"Data retrieved for {start} to {end}")
                json_data = response.json()
                if isinstance(json_data, list):  # If the response is a list
                    all_results.extend(json_data)
                elif isinstance(json_data, dict):  # If the response is a dictionary
                    all_results.extend(json_data.get('data', []))
                else:
                    print(f"Unexpected response format for {start} to {end}: {json_data}")
            else:
                print(f"Failed for {start} to {end}. Status Code: {response.status_code}")
                print(response.text)

            count += 1
        
    return all_results 

def get_monthly_app_performance_single_app_id(api_key, base_url, os, app_id, country_code, start_date, end_date):
    """
    Query monthly app performance data in 12-month segments if the range exceeds 12 months.
    """
    
    def split_date_range(start_date, end_date, max_months=12):
        """
        Split a date range into non-overlapping segments of up to `max_months` months.
        """
        current_start = datetime.strptime(start_date, '%Y-%m-%d')
        final_end = datetime.strptime(end_date, '%Y-%m-%d')
        date_ranges = []

        while current_start < final_end:
            # Calculate the end date for the current range
            current_end = current_start + timedelta(days=max_months * 30)  # Approximate 12 months
            # Ensure current_end does not exceed final_end
            current_end = min(current_end, final_end)
            # Adjust current_end to the last day of the month
            current_end = (current_end.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)

            # Add the range to the list
            date_ranges.append((current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')))

            # Move current_start to the first day of the next month after current_end
            current_start = (current_end + timedelta(days=1)).replace(day=1)

        return date_ranges
    
    print("Getting performance data for app id {}".format(app_id))

    endpoint = "/v1/{}/sales_report_estimates".format(os)
    url = f"{base_url}{endpoint}"

    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    date_ranges = split_date_range(start_date, end_date)
    
#     print("Date Ranges")
#     print(date_ranges)
    
    
    all_results = []

    for start, end in date_ranges:
        params = {
            'os': os,
            'app_ids': app_id,
            'countries': country_code,
            'date_granularity': 'monthly',
            'start_date': start,
            'end_date': end,
        }

        request = requests.Request("GET", url, headers=headers, params=params)
        prepared_request = request.prepare()

#         print("Requesting Data for Range:", start, "to", end)
#         print("Full Request URL:", prepared_request.url)

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            print(f"Data retrieved for {start} to {end}")
            json_data = response.json()
            if isinstance(json_data, list):  # If the response is a list
                all_results.extend(json_data)
            elif isinstance(json_data, dict):  # If the response is a dictionary
                all_results.extend(json_data.get('data', []))
            else:
                print(f"Unexpected response format for {start} to {end}: {json_data}")
        else:
            print(f"Failed for {start} to {end}. Status Code: {response.status_code}")
            print(response.text)
    
    return all_results

def get_performance_streams_from_list_of_app_ids(api_key, base_url, country_code, start_date, end_date, list_app_ids):
#     currently only get monthly performance
    result = []
    
    for app in list_app_ids:
        os = app["os"]
        app_id = app["app_id"]
        app_performance = get_monthly_app_performance_single_app_id(
            api_key, 
            base_url, 
            os, 
            app_id, 
            country_code,
            start_date, 
            end_date
        )
        result.append(
            {
                'app_id': app_id,
                'os': os,
                'app_performance': app_performance
            }
        )

    return result

def get_unique_dates(performance_streams_data):
    result = []
    for app in performance_streams_data:
        for record in app['app_performance']:
            if not(record['d'] in result):
                result.append(record['d'])
    return sorted(result)

def get_app_ids_from_performance_stream_data(performance_stream_data):
    return list(map(lambda x: x['app_id'], performance_stream_data))

def fill_performance_streams_df(blank_df, performance_streams_data):
    for row in performance_streams_data:
        if row['os'] == 'ios':
            for record in row['app_performance']:
                revenue_ar = record['ar']/100 if 'ar' in record else 0
                revenue_ir = record['ir']/100 if 'ir' in record else 0
                blank_df.at[row['app_id'], record['d']] = revenue_ar + revenue_ir
        elif row['os'] == 'android':
            for record in row['app_performance']:        
                blank_df.at[row['app_id'], record['d']] = record['r']/100 if 'r' in record else 0
    return blank_df

def generate_performance_streams_df(performance_streams_data):
#     prepare blank dataframe
    unique_dates = get_unique_dates(performance_streams_data)
    app_ids = get_app_ids_from_performance_stream_data(performance_streams_data)
    df = pd.DataFrame(index=app_ids, columns=unique_dates)
    
# fill the data
    return fill_performance_streams_df(df, performance_streams_data)

def get_daily_app_performance_single_unified_id(api_key, base_url, unified_id, country_code, start_date, end_date):
    """
    Fetch unified app data from the SensorTower API in batches of 100 IDs per request.
    
    Parameters:
        api_key (str): Your SensorTower API key.
        base_url (str): The base URL for the API.
        app_ids (list[str]): List of unified app IDs to fetch.
        
    Returns:
        list[dict]: Combined response data from all requests.
    """
    
    # API endpoint
    endpoint = "/v1/unified/sales_report_estimates"
    url = f"{base_url}{endpoint}"
    
    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    params = {
        'app_ids': unified_id,
        'countries': country_code,
        'date_granularity': 'daily',
        'start_date': start_date,
        'end_date': end_date,
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    return response.json()

def calc_download_revenue_sum_from_sales_report_estimates(sales_report):
    unified_id = sales_report[0]['app_id']
    start_date = sales_report[0]['date']
    end_date = sales_report[-1]['date']
    
    download_sum = 0
    revenue_sum = 0
    for row in sales_report:
        download_sum = download_sum + row['unified_units']
        revenue_sum = revenue_sum + row['unified_revenue']
    return {
        'unified_id': unified_id,
        'start_date': start_date,
        'end_date': end_date,
        'download': download_sum,
        'revenue': revenue_sum/100
    }

def get_first_n_day_sum_performance_single_unified_id(api_key, base_url, unified_id, country_code, release_date, n):

    query_result = get_daily_app_performance_single_unified_id(
        api_key = api_key, 
        base_url = base_url, 
        unified_id = unified_id, 
        country_code = country_code, 
        start_date = release_date, 
        end_date = add_days_to_date(start_date, n)
    )
    
    print(query_result)
    
    return calc_download_revenue_sum_from_sales_report_estimates(query_result)

def transform_df_local_app_info_to_dict_unified_id_local_app_info(df_local_app_info):
    table_app_ids = df_local_app_info.to_dict(orient='records')
    
    # get the list of unique unified_ids
    set_unified_ids = set(map(lambda x: x['unified_app_id'], table_app_ids))
    
    # Initiate the data structure unified_id: list of app of id info
    dict_unified_id_app_ids = dict()    
    
    for unified_id in set_unified_ids:
        dict_unified_id_app_ids[unified_id] = list(filter(lambda x: x['unified_app_id'] == unified_id, table_app_ids))
    
    return dict_unified_id_app_ids

def filter_vn_app_id(app_id_list):
    result = []
    for app in app_id_list:
        if 'VN' in parse_string_to_list(app['valid_countries']) or 'VN' in parse_string_to_list(app['top_countries']):
            result.append(app)
    return result

def get_top_games_by_year(df, percentile):
    """
    Get top games (Unified ID) contributing 90% of total revenue for each year.
    
    Args:
        df (DataFrame): Input DataFrame containing game performance data.
    
    Returns:
        dict: Dictionary with years as keys and lists of Unified IDs as values.
    """
    # Ensure the 'Date' column is datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Extract the year from the 'Date' column
    df['Year'] = df['Date'].dt.year

    # Initialize result dictionary
    top_games_by_year = {}

    # Iterate through each year in the dataset
    for year, group in df.groupby('Year'):
        # Total revenue for the year
        total_revenue = group['Revenue (Absolute, $)'].sum()

        # Sort by revenue in descending order
        group = group.sort_values(by='Revenue (Absolute, $)', ascending=False)

        # Calculate cumulative revenue and cumulative percentage
        group['Cumulative Revenue'] = group['Revenue (Absolute, $)'].cumsum()
        group['Cumulative %'] = group['Cumulative Revenue'] / total_revenue

        # Get the IDs that contribute up to <percentile>% of the revenue
        top_ids = group.loc[group['Cumulative %'] <= percentile, 'Unified ID'].tolist()

        # Store the result for the year
        top_games_by_year[year] = top_ids

    return top_games_by_year

def extract_all_values(dictionary):
    """
    Extract all values from a dictionary into a single list.
    
    Args:
        dictionary (dict): Input dictionary with values being lists.
    
    Returns:
        list: A list containing all values across all keys.
    """
    return [value for values in dictionary.values() for value in values]

# A function that takes in the set of unified_id 
# then output the full-info table of those unified_id (canonical, game tags and stuff)

def retrieve_full_info_game_table(api_key, base_url, set_unified_ids):
    
    print("Get canonical app data...")
    
    canonical_app_data = get_unified_app_data(
        api_key = api_key, 
        base_url = base_url, 
        app_ids = list(set_unified_ids)
    )
    
    df_canonical_app_data = pd.DataFrame(canonical_app_data)
    
    canonical_id_list = list(df_canonical_app_data['canonical_app_id'])
    
    canonical_id_list_fixed = [str(item) for item in canonical_id_list]
    
    print("Get game tags...")
    
    tags_data = get_required_tags(
        api_key, 
        base_url, 
        canonical_id_list_fixed
    )
    
    
    
    tags_table = get_tags_table(tags_data)
    
    df_tags_table = pd.DataFrame(tags_table)
    
    df_game_full_info = pd.merge(
        df_tags_table,
        df_canonical_app_data,
        how = 'left',
        on = 'canonical_app_id'
    )
    
    new_order = [
        'unified_app_id',
        'canonical_app_id',
        'name',
        'cohort_id',
        'itunes_apps',
        'android_apps',
        'unified_publisher_ids',
        'itunes_publisher_ids',
        'android_publisher_ids',
        'game_class',
        'game_genre',
        'game_subgenre',
        'game_art_style',
        'game_camera_pov',
        'game_setting',
        'game_theme',
        'game_product_model',
        'game_ip_corporate_parent',
        'game_ip_operator',
        'game_ip_media_type',
        'game_licensed_ip',
        'game_earliest_release_date',
        'game_release_date_ww',
        'game_release_date_us',
        'game_release_date_jp',
        'game_release_date_cn'
    ]
    
    return df_game_full_info[new_order]

def compare_dataframes(df1, df2):
    """
    Compare two DataFrames cell by cell and record differences.

    Args:
        df1 (DataFrame): First DataFrame to compare.
        df2 (DataFrame): Second DataFrame to compare.

    Returns:
        list: A list of dictionaries containing the differences with details.
    """
    # Ensure both DataFrames have the same shape
    if df1.shape != df2.shape:
        raise ValueError("DataFrames do not have the same shape.")

    differences = []

    # Iterate over each cell in the DataFrame
    for row_idx in range(len(df1)):
        for col in df1.columns:
            if df1.at[row_idx, col] != df2.at[row_idx, col]:
                differences.append({
                    'Row': row_idx,
                    'Column': col,
                    'Value_df1': df1.at[row_idx, col],
                    'Value_df2': df2.at[row_idx, col]
                })
    
    return differences

def flatten_app_list_of_dicts(list_of_dict):
    result = []
    for item in list_of_dict:
        result.append(str(item['app_id']))
    return result

def plot_performance_streams_df(df_original):
    
    df = df_original.copy()
    
#     # Convert columns to datetime for proper handling
#     df.columns = pd.to_datetime(df.columns)

#     # Plot individual app revenue time series
#     plt.figure(figsize=(12, 6))
#     for app in df.index:
#         plt.plot(df.columns, df.loc[app], marker='o', label=f'{app}')
#         # Add data labels to each point
#         for x, y in zip(df.columns, df.loc[app]):
#             plt.annotate(f'{y}', (x, y), textcoords="offset points", xytext=(0, 5), ha='center')

#     # Plot total revenue time series
#     total_revenue = df.sum(axis=0)
#     plt.plot(df.columns, total_revenue, marker='o', linestyle='--', color='black', label='Total Revenue')
#     # Add data labels for total revenue
#     for x, y in zip(df.columns, total_revenue):
#         plt.annotate(f'{y}', (x, y), textcoords="offset points", xytext=(0, 5), ha='center')

#     # Customize the plot
#     plt.title('Revenue Trends of Individual Apps and Total Revenue')
#     plt.xlabel('Time')
#     plt.ylabel('Revenue')
#     plt.xticks(df.columns, rotation=45)
#     plt.legend()
#     plt.tight_layout()
#     plt.grid(True)

#     # Show the plot
#     plt.show()
    
    
    
    # Convert column names (quarters) to datetime
    df.columns = pd.to_datetime(df.columns)

    # Transpose for easier time-based plotting
    df_transposed = df.T

    # Plot each app's time series
    plt.figure(figsize=(12, 6))
    for app_id in df_transposed.columns:
        plt.plot(df_transposed.index, df_transposed[app_id], label=f'App: {app_id}')

    # Calculate and plot total revenue
    total_revenue = df_transposed.sum(axis=1)
    plt.plot(df_transposed.index, total_revenue, label='Total Revenue', color='black', linewidth=2, linestyle='--')

    # Add plot labels and legend
    plt.title('Revenue Time Series of Apps')
    plt.xlabel('Time')
    plt.ylabel('Revenue')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Show the plot
    plt.show()

def plot_interactive_performance_streams_df(df_original):
    
    df = df_original.copy()
    
    # Create a Plotly Figure
    fig = go.Figure()

    # Add lines for individual apps
    for app in df.index:
        fig.add_trace(go.Scatter(
            x=df.columns, 
            y=df.loc[app], 
            mode='lines+markers', 
            name=f'{app}',
            text=df.loc[app],
            hovertemplate='App: %{name}<br>Date: %{x}<br>Revenue: %{y}<extra></extra>'
        ))

    # Add a line for total revenue
    total_revenue = df.sum(axis=0)
    fig.add_trace(go.Scatter(
        x=df.columns, 
        y=total_revenue, 
        mode='lines+markers', 
        name='Total Revenue',
        line=dict(dash='dot', color='black'),
        text=total_revenue,
        hovertemplate='Total Revenue<br>Date: %{x}<br>Revenue: %{y}<extra></extra>'
    ))

    # Customize layout
    fig.update_layout(
        title='Revenue Trends of Individual Apps and Total Revenue',
        xaxis_title='Time',
        yaxis_title='Revenue',
        xaxis=dict(rangeslider=dict(visible=True), type='date'),
        hovermode='x unified',
        template='plotly_white',
        legend=dict(title="Apps"),
    )

    # Show interactive plot
    fig.show()

def get_adjusted_yearly_revenue(revenue_df, app_info_df):
    
    def get_revenue_multiplier_of_app_id(app_id):
        return list(app_info_df[
            app_info_df[
                'app_id'].astype(str) == str(app_id)
        ]['revenue_multiplier'])[0]
    
    revenue_multiplier_vector = list(
        map(
            lambda x: get_revenue_multiplier_of_app_id(x),
            list(revenue_df.index)
        )
    )
    
    revenue_df_adjusted = revenue_df.mul(revenue_multiplier_vector, axis=0)
    
    revenue_df_adjusted_sum = revenue_df_adjusted.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    revenue_df_adjusted_sum.index = pd.to_datetime(revenue_df_adjusted_sum.index)

    # Group by year and sum the revenue for each year
    yearly_revenue = revenue_df_adjusted_sum.resample('Y').sum()

    # Convert the index to year only
    yearly_revenue.index = yearly_revenue.index.year
    
    return yearly_revenue

def get_yearly_revenue_no_multiplier(revenue_df, app_info_df):
    
    revenue_df_sum = revenue_df.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    revenue_df_sum.index = pd.to_datetime(revenue_df_sum.index)

    # Group by year and sum the revenue for each year
    yearly_revenue = revenue_df_sum.resample('Y').sum()

    # Convert the index to year only
    yearly_revenue.index = yearly_revenue.index.year
    
    return yearly_revenue

def get_adjusted_daily_revenue(revenue_df, app_info_df):
    
    def get_revenue_multiplier_of_app_id(app_id):
        return list(app_info_df[
            app_info_df[
                'app_id'].astype(str) == str(app_id)
        ]['revenue_multiplier'])[0]
    
    revenue_multiplier_vector = list(
        map(
            lambda x: get_revenue_multiplier_of_app_id(x),
            list(revenue_df.index)
        )
    )
    
    revenue_df_adjusted = revenue_df.mul(revenue_multiplier_vector, axis=0)
    
    revenue_df_adjusted_sum = revenue_df_adjusted.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    revenue_df_adjusted_sum.index = pd.to_datetime(revenue_df_adjusted_sum.index)

    # Convert the index to date format (YYYY-MM-DD)
    revenue_df_adjusted_sum.index = revenue_df_adjusted_sum.index.date
    
    return revenue_df_adjusted_sum

def get_daily_revenue_no_multiplier(revenue_df):
    
    revenue_df_sum = revenue_df.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    revenue_df_sum.index = pd.to_datetime(revenue_df_sum.index)

    # Convert the index to date format (YYYY-MM-DD)
    revenue_df_sum.index = revenue_df_sum.index.date
    
    return revenue_df_sum

def transform_raw_revenue_dataframe(df_input):
    
    df = df_input.copy()
    
    # Ensure the 'Date' column is in datetime format
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Extract the year from the 'Date' column
    df['Year'] = df['Date'].dt.year
    
    # Pivot the DataFrame
    transformed_df = df.pivot(index='Unified ID', columns='Year', values='Revenue (Absolute, $)')
    
    # Sort columns chronologically (ascending order by year)
    transformed_df = transformed_df.sort_index(axis=1, ascending=True)
    
    # Reset the column names (optional, to remove "Year" label)
    transformed_df.columns.name = None
    
    # Sort by the latest year (the last column after sorting columns by year)
    latest_year = transformed_df.columns[-1]  # Get the latest year
    transformed_df = transformed_df.sort_values(by=latest_year, ascending=False)
    
    return transformed_df

def adjust_revenue_df(original_df, replacement_data):
    """
    Replace the revenue series for specific game IDs in a DataFrame.
    
    Parameters:
    - original_df: The original DataFrame where index is 'unified_app_id' and columns are years.
    - replacement_data: A list of dictionaries with 'unified_app_id' and 'yearly_sum_performance_series'.
    
    Returns:
    - A new DataFrame with updated revenue values.
    """
    # Create a copy to avoid modifying the original DataFrame
    updated_df = original_df.copy()
    
    for replacement in replacement_data:
        unified_app_id = replacement['unified_app_id']
        new_series = replacement['yearly_sum_performance_series']
        
        # Replace the row corresponding to the unified_app_id with the new series
        for year, value in new_series.items():
            updated_df.at[unified_app_id, year] = value
            
    return updated_df

def get_top_items_contributing_x_pct_from_series(series_input, x):
    s = series_input.copy()
    total = s.sum()
    s = s.sort_values(ascending = False)
    s_cumsum = s.fillna(0).cumsum()
    s_cumulative_pct = s_cumsum/total
    return list(s_cumulative_pct[s_cumulative_pct <= x].index)

def get_unified_ids_contributing_x_pct_revenue_each_year(df_game_revenue_adjusted, x):
    
    top_items_contributing_x_pct_revenue_per_year = dict()

    for year in df_game_revenue_adjusted.columns:
        top_items_contributing_x_pct_revenue_per_year[year] = get_top_items_contributing_x_pct_from_series(df_game_revenue_adjusted[year], x)
    
    all_year_set = set()

    for id_list in top_items_contributing_x_pct_revenue_per_year.values():
        all_year_set.update(id_list)

    top_items_contributing_x_pct_revenue_per_year['all_year'] = list(all_year_set)
    
    return top_items_contributing_x_pct_revenue_per_year

def process_game_taxonomy(file_path):
    # Read the Excel file with the given path
    taxonomy_tree = pd.read_excel(file_path, sheet_name='taxonomy_tree')
    definition = pd.read_excel(file_path, sheet_name='definition')

    # Create a dictionary with a composite key of (item, level)
    definition_dict = {(row['item'], row['level']): row['definition'] for _, row in definition.iterrows()}

    # Process the data into the nested structure
    st_game_taxonomy = []

    # Group by game_class to start building the hierarchy
    for game_class, class_group in taxonomy_tree.groupby('game_class'):
        game_class_key = (game_class, 'game_class')
        game_class_dict = {
            'game_class': game_class,
            'definition': definition_dict.get(game_class_key, ''),
            'genres': []
        }
        
        # Group by game_genre within the game_class
        for game_genre, genre_group in class_group.groupby('game_genre'):
            game_genre_key = (game_genre, 'game_genre')
            game_genre_dict = {
                'game_genre': game_genre,
                'definition': definition_dict.get(game_genre_key, ''),
                'subgenres': []
            }
            
            # Add subgenres to the genre
            for _, row in genre_group.iterrows():
                game_subgenre_key = (row['game_subgenre'], 'game_subgenre')
                game_subgenre_dict = {
                    'game_subgenre': row['game_subgenre'],
                    'definition': definition_dict.get(game_subgenre_key, '')
                }
                game_genre_dict['subgenres'].append(game_subgenre_dict)
            
            game_class_dict['genres'].append(game_genre_dict)
        
        st_game_taxonomy.append(game_class_dict)
    
    return st_game_taxonomy

def transform_df_local_app_info_to_dict_unified_id_local_app_info(df_local_app_info):
    table_app_ids = df_local_app_info.to_dict(orient='records')
    
    # get the list of unique unified_ids
    set_unified_ids = set(map(lambda x: x['unified_app_id'], table_app_ids))
    
    # Initiate the data structure unified_id: list of app of id info
    dict_unified_id_app_ids = dict()    
    
    for unified_id in set_unified_ids:
        dict_unified_id_app_ids[unified_id] = list(filter(lambda x: x['unified_app_id'] == unified_id, table_app_ids))
    
    return dict_unified_id_app_ids

def filter_vn_app_id(app_id_list):
    result = []
    for app in app_id_list:
        if 'VN' in parse_string_to_list(app['valid_countries']) or 'VN' in parse_string_to_list(app['top_countries']):
            result.append(app)
    return result

def get_release_date_from_unified_id(df_local_app_info, df_unified_full_info, unified_id):
    
#     deprecated due to old algorithm

    dict_unified_id_local_app_info = transform_df_local_app_info_to_dict_unified_id_local_app_info(df_local_app_info)
    
    list_local_app_info_vn = filter_vn_app_id(dict_unified_id_local_app_info[unified_id])
    
    if not list_local_app_info_vn:
        return string_to_timestamp_v2(df_unified_full_info[df_unified_full_info['unified_app_id'] == unified_id]['game_earliest_release_date'])
    else:
        return min(list(map(lambda x: string_to_timestamp(x['country_release_date']), list_local_app_info_vn)))
    

def get_release_year(dataframe, unified_id):
    
#     Input dataframe = df_game_revenue_adjusted (index being the unified ids, columns being the years)

    """
    Finds the starting year of the revenue time series for a specific game.

    Args:
        dataframe (pd.DataFrame): The input table with revenue time series data, 
                                  with Unified ID as the index.
        unified_id (str): The Unified ID of the game.

    Returns:
        int: The starting year (release year) of the game, or None if no revenue is found.
    """
    # Check if the Unified ID exists in the index
    if unified_id not in dataframe.index:
        print(f"Game with Unified ID {unified_id} not found.")
        return None

    # Extract the revenue time series for the given Unified ID
    row_revenue = dataframe.loc[unified_id]

    # Find the first year with non-NaN and non-zero revenue
    for year, revenue in row_revenue.items():
        if not pd.isna(revenue) and revenue > 0:
            return int(year)

    # If no revenue is found, return None
    return None

def get_release_date_from_daily_game_performance_table(table, unified_id):
#     not the full revenue table like input of the function get_release_year
    
    print("Processing unified app id: {}".format(unified_id))
    
    date_series = list(
        filter(
            lambda x: x['unified_app_id'] == unified_id,
            table
        )
    )[0]['daily_adjusted_revenue_series']
    
    if date_series.empty:
        return None
    
    return date_series.index[0]

# Previous version

# def calculate_first_n_day_revenue(daily_sum_performance_df_for_games, unified_app_id, n):
#     """
#     Calculate the first N-day revenue for a specific game.

#     Args:
#         daily_sum_performance_df_for_games (list): List of dictionaries containing game data.
#         unified_app_id (str): The unified app ID of the game.
#         n (int): The number of days (N) to calculate the revenue for.

#     Returns:
#         float or None: The first N-day revenue for the specified game, or None if the game is not found
#                        or doesn't have enough data points.
#     """
#     # Find the game with the specified unified_app_id
#     for game_data in daily_sum_performance_df_for_games:
#         if game_data['unified_app_id'] == unified_app_id:
#             # Get the Pandas Series for the game's daily revenue
#             daily_revenue_series = game_data['daily_adjusted_revenue_series']
            
#             # Sort the Series by the index to ensure chronological order
#             daily_revenue_series = daily_revenue_series.sort_index()
            
#             # Check if the revenue series has at least N data points
#             if len(daily_revenue_series) < n:
#                 print(f"Game {unified_app_id} does not have enough data points for {n}-day revenue.")
#                 return None
            
#             # Calculate the first N-day revenue
#             first_n_day_revenue = daily_revenue_series.head(n).sum()
#             return first_n_day_revenue
    
#     # Return None if the game with the specified ID is not found
#     print(f"Game with unified_app_id {unified_app_id} not found.")
#     return None

def calculate_first_n_day_revenue(daily_sum_performance_df_for_games, unified_app_id, n):
    """
    Calculate the first N-day revenue for a specific game, accounting for gaps in data.

    Args:
        daily_sum_performance_df_for_games (list): List of dictionaries containing game data.
        unified_app_id (str): The unified app ID of the game.
        n (int): The number of days (N) to calculate the revenue for.

    Returns:
        float or None: The first N-day revenue for the specified game, or None if the game is not found
                       or the date range does not span N days.
    """
    
    print("Processing for unified app id {}".format(unified_app_id))
    
    for game_data in daily_sum_performance_df_for_games:
        if game_data['unified_app_id'] == unified_app_id:
            # Get the Pandas Series for the game's daily revenue
            daily_revenue_series = game_data['daily_adjusted_revenue_series']
            
            # Check whether the series is empty
            if daily_revenue_series.empty:
                print(f"Game {unified_app_id} revenue series is empty.")
                return None
            
            # Sort the Series by the index to ensure chronological order
            daily_revenue_series = daily_revenue_series.sort_index()

            # Get the first and last date in the series
            start_date = daily_revenue_series.index.min()
            end_date = daily_revenue_series.index.max()
            
            # Get today date
            today_date = date.today()

            # Check if the date range spans at least N days
            date_range_days = (end_date - start_date).days + 1
            if date_range_days < n:
                if (today_date - start_date).days + 1 >= n:
                    return daily_revenue_series.sum()
                else:
                    print(f"Game {unified_app_id} does not have a date range spanning {n} days.")
                    return None

            # Calculate the first N-day revenue based on the date range
            valid_revenue = daily_revenue_series[start_date: start_date + pd.Timedelta(days=n-1)]
            first_n_day_revenue = valid_revenue.sum()
            return first_n_day_revenue

    # Return None if the game with the specified ID is not found
    print(f"Game with unified_app_id {unified_app_id} not found.")
    return None

def calculate_performance_for_new_games(daily_sum_performance_df_for_games, new_games_ids):
    """
    Calculate the first 7-day, 30-day, and 90-day revenue for new games.

    Args:
        daily_sum_performance_df_for_games (list): List of dictionaries containing game data.
        new_games_ids (list): List of unified_app_id for new games.

    Returns:
        list: A list of dictionaries containing unified_app_id and calculated revenue metrics.
    """
    performance_list = []

    for game_id in new_games_ids:
        # Initialize the dictionary for the game
        game_performance = {'unified_app_id': game_id}
        
        # Calculate revenue for 7-day, 30-day, and 90-day
        game_performance['first 7-day revenue'] = calculate_first_n_day_revenue(daily_sum_performance_df_for_games, game_id, 7)
        game_performance['first 30-day revenue'] = calculate_first_n_day_revenue(daily_sum_performance_df_for_games, game_id, 30)
        game_performance['first 90-day revenue'] = calculate_first_n_day_revenue(daily_sum_performance_df_for_games, game_id, 90)
        
        # Add the result to the list
        performance_list.append(game_performance)

    return performance_list

def update_game_classification(df_input, df_genre_validation_gpt_output_manually_corrected):
    """
    Updates the 'game_class', 'game_genre', and 'game_subgenre' columns in `df_game_full_info` 
    based on the corrected classifications in `df_genre_validation_gpt_output_manually_corrected`.

    Args:
        df_game_full_info (pd.DataFrame): The main dataframe containing game information.
        df_genre_validation_gpt_output_manually_corrected (pd.DataFrame): 
            The dataframe with corrected classification data.

    Returns:
        pd.DataFrame: Updated `df_game_full_info` with corrected classification for certain games.
    """
    
    df_game_full_info = df_input.copy()
    
    # Create a mapping of game_id to the finalized classifications
    corrected_classifications = df_genre_validation_gpt_output_manually_corrected[
        ['game_id', 'finalized_game_class', 'finalized_game_genre', 'finalized_game_subgenre']
    ].set_index('game_id')

    # Ensure game_id exists in both DataFrames
    df_game_full_info.set_index('unified_app_id', inplace=True)
    
    # Update the game_class, game_genre, and game_subgenre based on the corrected data
    df_game_full_info.update(corrected_classifications.rename(columns={
        'finalized_game_class': 'game_class',
        'finalized_game_genre': 'game_genre',
        'finalized_game_subgenre': 'game_subgenre'
    }))
    
    # Reset index to preserve the original structure
    df_game_full_info.reset_index(inplace=True)
    
    return df_game_full_info

def update_game_classification_for_revenue_df(df_input, additional_genre_data):
    """
    Updates the genre classification information in a DataFrame with additional data.

    Args:
        df (pd.DataFrame): The DataFrame containing game revenue and classification info.
        additional_genre_data (list of dict): List of dictionaries with additional genre data.

    Returns:
        pd.DataFrame: Updated DataFrame with genre classification information.
    """
    
    df = df_input.copy()
    
    # Convert the list of dictionaries to a temporary DataFrame
    additional_genre_df = pd.DataFrame(additional_genre_data)
    
    # Rename columns in the temporary DataFrame to match the target DataFrame
    additional_genre_df.rename(columns={
        'game_id': 'Unified ID',
        'finalized_game_class': 'game_class',
        'finalized_game_genre': 'game_genre',
        'finalized_game_subgenre': 'game_subgenre'
    }, inplace=True)
    
    # Set 'Unified ID' as the index for both DataFrames to facilitate alignment
    additional_genre_df.set_index('Unified ID', inplace=True)
    
    # Update the main DataFrame with the additional genre data
    df.update(additional_genre_df)

    return df

def join_game_genres_to_revenue(df_game_revenue_adjusted, df_game_full_info_with_corrected_genres):
    """
    Joins the fields 'game_class', 'game_genre', and 'game_subgenre' from the 
    df_game_full_info_with_corrected_genres dataframe to the df_game_revenue_adjusted dataframe
    and moves the newly added columns to the leftmost position.

    Args:
        df_game_revenue_adjusted (pd.DataFrame): The dataframe with revenue data by year.
        df_game_full_info_with_corrected_genres (pd.DataFrame): 
            The dataframe with game information.
        
    Returns:
        pd.DataFrame: Updated dataframe with 'game_class', 'game_genre', and 'game_subgenre' 
                      fields added at the leftmost position.
    """
    # Ensure 'unified_app_id' is the index in df_game_full_info_with_corrected_genres
    df_genres = df_game_full_info_with_corrected_genres.set_index('unified_app_id')[
        ['game_class', 'game_genre', 'game_subgenre']
    ]

    # Join the genre information to the revenue dataframe
    df_merged = df_game_revenue_adjusted.join(df_genres, how='left')

    # Move the new columns to the leftmost position
    new_columns = ['game_class', 'game_genre', 'game_subgenre']
    reordered_columns = new_columns + [col for col in df_merged.columns if col not in new_columns]
    df_merged = df_merged[reordered_columns]

    return df_merged

def get_monthly_revenue_series(daily_sum_performance, unified_app_id):
    """
    Convert the daily revenue series of a game to a monthly revenue series.

    Args:
        daily_sum_performance (list of dict): The data structure containing game daily revenue.
        unified_app_id (str): The unified_app_id of the game.

    Returns:
        dict: A dictionary with 'unified_app_id' and 'monthly_adjusted_revenue_series'.
    """
    # Find the game with the specified unified_app_id
    for game_data in daily_sum_performance:
        if game_data['unified_app_id'] == unified_app_id:
            # Get the daily revenue series
            daily_series = game_data['daily_adjusted_revenue_series']
            
            # Ensure the index is a datetime index
            daily_series.index = pd.to_datetime(daily_series.index)
            
            # Resample to monthly frequency and sum the revenues
            monthly_series = daily_series.resample('M').sum()
            
            return {
                'unified_app_id': unified_app_id,
                'monthly_adjusted_revenue_series': monthly_series
            }
    
    # If the game is not found, return None or raise an exception
    print(f"Game with unified_app_id '{unified_app_id}' not found.")
    return None

def calculate_average_30_day_revenue(daily_sum_performance, unified_app_id):
    """
    Calculate the 30-day average revenue for a game based on daily data.
    
    Args:
        daily_sum_performance (list of dict): List of dictionaries with daily revenue series.
        unified_app_id (str): The unified_app_id of the game.
    
    Returns:
        dict: A dictionary containing:
            - 'unified_app_id': The game ID.
            - 'average_30_day_revenue': The average 30-day revenue or None if not enough data.
    """
    
    # Find the game with the specified unified_app_id
    for game_data in daily_sum_performance:
        if game_data['unified_app_id'] == unified_app_id:
            # Get the daily revenue series
            daily_series = game_data['daily_adjusted_revenue_series']
            
            # Check if there are at least 180 data points
            if len(daily_series) >= 180:
                # Calculate the average daily revenue
                avg_daily_revenue = daily_series.mean()
                # Calculate the average 30-day revenue
                avg_30_day_revenue = avg_daily_revenue * 30
                return {
                    'unified_app_id': unified_app_id,
                    'average_30_day_revenue': avg_30_day_revenue
                }
            else:
                # Not enough data to calculate the average
                return {
                    'unified_app_id': unified_app_id,
                    'average_30_day_revenue': None
                }
    
    # If the game is not found, return None
    print(f"Game with unified_app_id '{unified_app_id}' not found.")
    return None

def fill_download_streams_df(blank_df, performance_streams_data):
    for row in performance_streams_data:
        if row['os'] == 'ios':
            for record in row['app_performance']:
                download_iu = record['iu'] if 'iu' in record else 0
                download_au = record['au'] if 'au' in record else 0
                blank_df.at[row['app_id'], record['d']] = download_iu + download_au
        elif row['os'] == 'android':
            for record in row['app_performance']:        
                blank_df.at[row['app_id'], record['d']] = record['u'] if 'u' in record else 0
    return blank_df

def generate_download_streams_df(performance_streams_data):
#     prepare blank dataframe
    unique_dates = get_unique_dates(performance_streams_data)
    app_ids = get_app_ids_from_performance_stream_data(performance_streams_data)
    df = pd.DataFrame(index=app_ids, columns=unique_dates)
    
# fill the data
    return fill_download_streams_df(df, performance_streams_data)

def get_daily_download(download_df):
    
    # Sum across the downloads of different apps of the same game
    download_df_sum = download_df.fillna(0).sum(axis=0)
    
    # Convert index to datetime
    download_df_sum.index = pd.to_datetime(download_df_sum.index)

    # Convert the index to date format (YYYY-MM-DD)
    download_df_sum.index = download_df_sum.index.date

    return download_df_sum

def transform_game_download_list_of_dict_to_dataframe(the_list):
    
# the input list can be in the form of [
#     {
#      'unified_app_id',
#      'daily_download_series'
#     }
# ]
    
    # Convert list of dictionaries to a dictionary of Series
    the_dict = {entry['unified_app_id']: entry['daily_download_series'] for entry in the_list}
    
    # Convert to DataFrame
    download_df = pd.DataFrame(the_dict).T  # Transpose to make game IDs rows
    
    return download_df

def calculate_first_n_day_download(daily_sum_download_df_for_games, unified_app_id, n):
    
    print("Processing for unified app id {}".format(unified_app_id))
    
    for game_data in daily_sum_download_df_for_games:
        if game_data['unified_app_id'] == unified_app_id:
            # Get the Pandas Series for the game's daily revenue
            daily_download_series = game_data['daily_download_series']
            
            # Check whether the series is empty
            if daily_download_series.empty:
                print(f"Game {unified_app_id} download series is empty.")
                return None
            
            # Sort the Series by the index to ensure chronological order
            daily_download_series = daily_download_series.sort_index()

            # Get the first and last date in the series
            start_date = daily_download_series.index.min()
            end_date = daily_download_series.index.max()
            
            # Get today date
            today_date = date.today()

            # Check if the date range spans at least N days
            date_range_days = (end_date - start_date).days + 1
            if date_range_days < n:
                if (today_date - start_date).days + 1 >= n:
                    return daily_download_series.sum()
                else:
                    print(f"Game {unified_app_id} does not have a date range spanning {n} days.")
                    return None

            # Calculate the first N-day revenue based on the date range
            valid_download = daily_download_series[start_date: start_date + pd.Timedelta(days=n-1)]
            first_n_day_download = valid_download.sum()
            return first_n_day_download

    # Return None if the game with the specified ID is not found
    print(f"Game with unified_app_id {unified_app_id} not found.")
    return None

def get_release_date_from_daily_game_download_table(table, unified_id):
#     not the full revenue table like input of the function get_release_year
    
    print("Processing unified app id: {}".format(unified_id))
    
    date_series = list(
        filter(
            lambda x: x['unified_app_id'] == unified_id,
            table
        )
    )[0]['daily_download_series']
    
    if date_series.empty:
        return None
    
    return date_series.index[0]
    
def calculate_revenue_and_growth_by_classification(df_revenue_with_genres, classification):
    """
    Calculate total revenue, YoY growth, and CAGR for revenue grouped by 'game_class'.

    Args:
        df_revenue_with_genres (pd.DataFrame): Revenue data with 'game_class', 'game_genre', 'game_subgenre'.
        classification (str): Classification to group by ('class', 'genre', 'subgenre').

    Returns:
        tuple: Two DataFrames:
            - Total revenue by game class.
            - Growth and CAGR by game class.
    """

    classifications = ['class', 'genre', 'subgenre']
    if classification not in classifications:
        raise Exception('Invalid classification. Must be in the list {}'.format(classifications))

    full_classification_list = ['game_' + cls for cls in classifications]

    if classification == 'class':
        to_drop_list = ['game_genre', 'game_subgenre']
        groupby_list = ['game_class']
    elif classification == 'genre':
        to_drop_list = ['game_subgenre']
        groupby_list = ['game_class', 'game_genre']
    else:
        to_drop_list = []
        groupby_list = full_classification_list

    # Group by classification and sum revenue
    total_revenue = df_revenue_with_genres.drop(
        to_drop_list, axis=1
    ).groupby(groupby_list).sum()

    # Calculate YoY growth (proportions, not percentages)
    yoy_growth = total_revenue.pct_change(axis=1) * 100

    # Calculate CAGR with dynamic start year
    cagr_results = {}
    for game_class, revenue_row in total_revenue.iterrows():
        # Drop NaN values
        revenue_row = revenue_row.dropna()

        # Dynamically determine the first year with non-zero revenue
        for year in revenue_row.index:
            if revenue_row[year] > 0:
                actual_start_year = year
                break
        else:
            # If no non-zero revenue found, skip this group
            cagr_results[game_class] = None
            continue

        # Determine the end year
        actual_end_year = revenue_row.index.max()
        revenue_start = revenue_row[actual_start_year]
        revenue_end = revenue_row[actual_end_year]

        if actual_start_year == actual_end_year or revenue_start == 0 or revenue_end == 0:
            # Cannot calculate CAGR for only one year or if revenue is zero
            cagr_results[game_class] = None
        else:
            num_years = int(actual_end_year) - int(actual_start_year)
            cagr_results[game_class] = ((revenue_end / revenue_start) ** (1 / num_years) - 1) * 100

    # Create a DataFrame for CAGR
    cagr_df = pd.DataFrame.from_dict(cagr_results, orient='index', columns=['CAGR'])

    # Combine results into one DataFrame with only YoY growth and CAGR
    growth_and_cagr = pd.concat([yoy_growth.add_suffix('_YoY'), cagr_df], axis=1)

    return total_revenue, growth_and_cagr

def create_mapping_publisher_to_apps_publisherids(path_to_mapping_publisher_to_apps, api_key, base_url):
    print("Reading csv file: mapping publisher to apps...")
    df_store_account_data_top_down_manual_cleaning = pd.read_csv(path_to_mapping_publisher_to_apps)

    df_store_account_data_top_down_manual_cleaning["app_id_trimmed"] = (
        df_store_account_data_top_down_manual_cleaning["app_id_trimmed"].astype(str).str.strip()
    )

    lod_store_account_data_top_down_manual_cleaning = df_store_account_data_top_down_manual_cleaning.to_dict(orient='records')
    ios_app_id_list_store_account_top_down_manual_cleaning = list(map(lambda x: x['app_id_trimmed'], list(filter(lambda x: x['os'] == 'ios', lod_store_account_data_top_down_manual_cleaning))))
    android_app_id_list_store_account_top_down_manual_cleaning = list(map(lambda x: x['app_id_trimmed'], list(filter(lambda x: x['os'] == 'android', lod_store_account_data_top_down_manual_cleaning))))
    
    print("Get ST publisher info for ios apps...")
    ios_app_info_list_store_account_top_down_manual_cleaning = get_local_app_info(api_key, base_url, "ios", ios_app_id_list_store_account_top_down_manual_cleaning )
    
    print("Get ST publisher info for Android apps...")
    android_app_info_list_store_account_top_down_manual_cleaning = get_local_app_info(api_key, base_url, "android", android_app_id_list_store_account_top_down_manual_cleaning)

    df_all_app_info_store_account_top_down_manual_cleaning = pd.concat(
        [
            pd.DataFrame(ios_app_info_list_store_account_top_down_manual_cleaning),
            pd.DataFrame(android_app_info_list_store_account_top_down_manual_cleaning)
        ],
        ignore_index=True
    )

    df_all_app_info_store_account_top_down_manual_cleaning['app_id'] = df_all_app_info_store_account_top_down_manual_cleaning['app_id'].astype(str)

    df_store_account_data_top_down = pd.merge(
        df_store_account_data_top_down_manual_cleaning,
        df_all_app_info_store_account_top_down_manual_cleaning,
        left_on = "app_id_trimmed",
        right_on = "app_id",
        how = "left"
    )[['os_x', 'cleaned_publisher_name', 'game_name', 'sensor_tower_link', 'app_id_trimmed', 'publisher_id', 'publisher_name']]

    return df_store_account_data_top_down

def create_mapping_publisher_to_publisherids_revenue_multiplier(df_top_down, df_bottom_up):
    df_mapping_cleaned_publisher_to_revenue_multiplier = df_bottom_up[['cleaned_publisher_name', 'revenue_multiplier']].drop_duplicates().reset_index(drop = True)
    df_top_down_cleaning = pd.merge(
        df_top_down,
        df_mapping_cleaned_publisher_to_revenue_multiplier,
        on = 'cleaned_publisher_name',
        how = 'left'
    )[['cleaned_publisher_name', 'publisher_id', 'publisher_name', 'revenue_multiplier']].drop_duplicates()
    
    df_full = pd.concat(
        [df_bottom_up, df_top_down_cleaning]
    ).drop_duplicates().reset_index(drop=True)

    print("Merging is complete, now have the full mapping table. But need to double check for rows having null publisher_id")

    print("These are rows having null publisher_id:")

    df_null_pub_rows = df_full[df_full['publisher_id'].isnull()]

    print(df_null_pub_rows)

    print("Best to check again these rows. Potential reasons: apps become inactive in the country of interest; app id typo; app id changed by SensorTower; etc.")

    print("Removing these rows from the merged mapping table...")
    
    df_full = df_full[df_full['publisher_id'].notnull()]

    return df_full

def add_cleaned_publisher_name_and_revenue_multiplier_to_app_full_info(df_app_full_info, df_mapping_publisher_to_publiserid_and_revenue_multiplier_full):
    
    # Get the dictionary mapping table publisher_id - cleaned_publisher_name
    dict_publisher_id_cleaned_publisher_name = dict()
    for index, row in df_mapping_publisher_to_publiserid_and_revenue_multiplier_full.iterrows():
        dict_publisher_id_cleaned_publisher_name[str(row['publisher_id'])] = row['cleaned_publisher_name']

    # Get the mapping table publisher_id - multiplier
    dict_publisher_id_revenue_multiplier = dict()
    for index, row in df_mapping_publisher_to_publiserid_and_revenue_multiplier_full.iterrows():
        dict_publisher_id_revenue_multiplier[str(row['publisher_id'])] = row['revenue_multiplier']

    def cast_cleaned_publisher_name(row):
        if str(row['publisher_id']) in dict_publisher_id_cleaned_publisher_name.keys():
            return dict_publisher_id_cleaned_publisher_name[str(row['publisher_id'])]
        else:
            return ""
    
    def cast_revenue_multiplier(row):
        if str(row['publisher_id']) in dict_publisher_id_revenue_multiplier.keys():
            return dict_publisher_id_revenue_multiplier[str(row['publisher_id'])]
        else:
            return 1
    
    df_app_full_info_copy = df_app_full_info.copy()

    df_app_full_info_copy['cleaned_publisher_name'] = df_app_full_info_copy.apply(cast_cleaned_publisher_name, axis=1)
    df_app_full_info_copy['revenue_multiplier'] = df_app_full_info_copy.apply(cast_revenue_multiplier, axis=1)

    return df_app_full_info_copy

def adjust_cleaned_publisher_name_and_revenue_multiplier_of_app_full_info_special_cases(df_app_full_info_adjusted, df_mapping_app_to_revenue_multiplier_special_case):

    dict_mapping_app_to_revenue_multiplier_special_case = dict()
    for index, row in df_mapping_app_to_revenue_multiplier_special_case.iterrows():
        dict_mapping_app_to_revenue_multiplier_special_case[str(row['app_id'])] = {'cleaned_publisher_name': row['cleaned_publisher_name'], 'revenue_multiplier': row['revenue_multiplier']}
    
    def cast_adjusted_cleaned_publisher_name(row):
        if str(row['app_id']) in dict_mapping_app_to_revenue_multiplier_special_case.keys():
            return dict_mapping_app_to_revenue_multiplier_special_case[str(row['app_id'])]['cleaned_publisher_name']
        else:
            return row['cleaned_publisher_name']
    
    def cast_adjusted_revenue_multiplier(row):
        if str(row['app_id']) in dict_mapping_app_to_revenue_multiplier_special_case.keys():
            return dict_mapping_app_to_revenue_multiplier_special_case[str(row['app_id'])]['revenue_multiplier']
        else:
            return row['revenue_multiplier']
    
    df_app_full_info_adjusted_copy = df_app_full_info_adjusted.copy()

    df_app_full_info_adjusted_copy['cleaned_publisher_name'] = df_app_full_info_adjusted_copy.apply(cast_adjusted_cleaned_publisher_name, axis=1)
    df_app_full_info_adjusted_copy['revenue_multiplier'] = df_app_full_info_adjusted_copy.apply(cast_adjusted_revenue_multiplier, axis=1)

    return df_app_full_info_adjusted_copy

def create_table_app_performance_grouped_by_game_daily(
    api_key,
    base_url,
    str_start_date,
    str_end_date,
    df_app_full_info,
    json_export_path,
    export_path=None,
    country_codes="VN",
):
    # get the unified ids
    list_unified_ids = list(
        df_app_full_info['unified_app_id'].drop_duplicates()
    )
    
    ## get the ios app_ids
    list_app_ids_ios = list(
        map(lambda x: str(x),
            list(
                    df_app_full_info[
                        df_app_full_info['os'] == 'ios'
                    ]['app_id']
                )
            )
    )

    ## get the android app_ids
    list_app_ids_android = list(
        map(lambda x: str(x),
            list(
                    df_app_full_info[
                        df_app_full_info['os'] == 'android'
                    ]['app_id']
                )
            )
    )

    # Query the app performance data (performance piece - each in a period, for an app)

    print("Query app performance data pieces...")
    print(f"Countries: {country_codes} | Start: {str_start_date} | End: {str_end_date}")

    app_performance_data_ios = get_apps_performance(
        api_key, 
        base_url, 
        os = 'ios', 
        app_ids = list_app_ids_ios, 
        country_code = country_codes, 
        granularity = 'daily', 
        max_periods = 1000, 
        start_date = str_start_date, 
        end_date = str_end_date
    )

    app_performance_data_android = get_apps_performance(
        api_key, 
        base_url, 
        os = 'android', 
        app_ids = list_app_ids_android, 
        country_code = country_codes, 
        granularity = 'daily', 
        max_periods = 1000, 
        start_date = str_start_date, 
        end_date = str_end_date
    )
    
    ## join the 2 lists of performance data together
    print("Concatenating the Android and iOS lists...")
    app_performance_data = app_performance_data_ios + app_performance_data_android

    ## export the data to json
    print("Exporting the data to json...")
    if export_path is None:
        timestamp = int(time.time())
        export_path = '{}/st_app_performance_daily_{}.json'.format(json_export_path, timestamp)
    with open(export_path, 'w', encoding='utf-8') as file:
        json.dump(app_performance_data, file, ensure_ascii=False)

    return app_performance_data

def stream_and_adjust_app_performance_daily_json_file(src_path, dst_path, df_app_full_info_adjusted):
    
    # Get the mapping app_id => revenue_multiplier
    df_mult = df_app_full_info_adjusted[df_app_full_info_adjusted['revenue_multiplier']!=1][['app_id','revenue_multiplier']].copy()
    df_mult["app_id_str"] = df_mult["app_id"].astype(str)
    revenue_factor_by_app = (
        df_mult.set_index("app_id_str")["revenue_multiplier"].to_dict()
    )

    # Supporting function to adjust revenue of each record
    def adjust_revenue(rec):
        aid_str = str(rec.get("aid"))
        factor = revenue_factor_by_app.get(aid_str)

        # If no multiplier (or factor == 1), just return as-is
        if factor is None or factor == 1:
            return rec

        # iOS: ir = iPhone revenue, ar = iPad revenue
        if "ir" in rec and rec["ir"] is not None:
            rec["ir"] = rec["ir"] * factor
        if "ar" in rec and rec["ar"] is not None:
            rec["ar"] = rec["ar"] * factor

        # Android: r = Android revenue
        if "r" in rec and rec["r"] is not None:
            rec["r"] = rec["r"] * factor

        return rec
    
    # Streaming and adjusting original app performance data file to adjusted app performance data file
    print("Streaming and adjusting app performance daily file: {} => {}".format(src_path, dst_path))

    total_bytes = os.path.getsize(src_path)

    records_written = 0

    with open(src_path, "rb") as src, \
        open(dst_path, "w", encoding="utf-8") as dst, \
        tqdm(total=total_bytes, unit="B", unit_scale=True, desc="Processing") as pbar:

        dst.write("[\n")
        first = True

        for rec in ijson.items(src, "item"):
            rec = adjust_revenue(rec)

            if not first:
                dst.write(",\n")
            first = False

            json.dump(rec, dst, ensure_ascii=False)
            records_written += 1

            # update progress to current file position
            pbar.update(src.tell() - pbar.n)

        dst.write("\n]\n")

    return {
        "output_path": dst_path,
        "records_written": records_written,
    }

def apply_game_info_adjustments(df_game_full_info: pd.DataFrame,
                                df_game_info_adjustment: pd.DataFrame,
                                key: str = "unified_app_id") -> pd.DataFrame:
    """
    Return df_game_full_info with metadata updated by df_game_info_adjustment.

    - key: column used to identify games (default: 'unified_app_id')
    - Any columns present in df_game_info_adjustment will be used to update
      matching rows in df_game_full_info.
    - If df_game_info_adjustment has new columns not in df_game_full_info,
      those columns are added.
    - If multiple rows in df_game_info_adjustment share the same key,
      the *last* one is used.
    """
    # Work on copies so we don't modify original dataframes
    full = df_game_full_info.copy()
    adj = df_game_info_adjustment.copy()

    # Keep last adjustment per unified_app_id (if duplicates exist)
    adj = adj.drop_duplicates(subset=[key], keep="last")

    # Set index to the key for alignment
    full = full.set_index(key)
    adj = adj.set_index(key)

    # Ensure all adjustment columns exist in full; if not, create them
    for col in adj.columns:
        if col not in full.columns:
            full[col] = pd.NA

    # Update in-place: aligns on index (key) and column names
    full.update(adj)

    # Restore key as a normal column
    full = full.reset_index()

    return full

# New set of functions for adjusting App Full Info (NDJSON to NDJSON instead of csv to csv)

def build_publisher_maps(mapping_csv_path: str) -> Tuple[Dict[str, str], Dict[str, int]]:
    """
    Build:
      - dict_publisher_id_cleaned_publisher_name: publisher_id(str) -> cleaned_publisher_name(str)
      - dict_publisher_id_revenue_multiplier:    publisher_id(str) -> revenue_multiplier(int)
    Mirrors your pandas mapping logic (publisher_id-based).
    """
    dict_publisher_id_cleaned_publisher_name: Dict[str, str] = {}
    dict_publisher_id_revenue_multiplier: Dict[str, int] = {}

    with open(mapping_csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = str(row.get("publisher_id") or "").strip()
            if not pid:
                continue

            cleaned = row.get("cleaned_publisher_name")
            if cleaned is None:
                cleaned = ""
            cleaned = str(cleaned)

            rm = _to_int_or_none(row.get("revenue_multiplier"))
            if rm is None:
                rm = 1

            dict_publisher_id_cleaned_publisher_name[pid] = cleaned
            dict_publisher_id_revenue_multiplier[pid] = rm

    return dict_publisher_id_cleaned_publisher_name, dict_publisher_id_revenue_multiplier


def build_special_case_map(special_case_csv_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Build special-case map:
      app_id(str) -> {"cleaned_publisher_name": ..., "revenue_multiplier": ...}
    Mirrors your pandas special-case mapping logic.
    """
    out: Dict[str, Dict[str, Any]] = {}

    with open(special_case_csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            app_id = str(row.get("app_id") or "").strip()
            if not app_id:
                continue

            cleaned = row.get("cleaned_publisher_name")
            # keep as-is; could be empty string in your csv
            if cleaned is None:
                cleaned = ""

            rm = _to_int_or_none(row.get("revenue_multiplier"))
            # if missing in special-case row, we keep None so we can "not override" if you prefer
            out[app_id] = {
                "cleaned_publisher_name": str(cleaned),
                "revenue_multiplier": rm,
            }

    return out


def add_cleaned_publisher_name_and_revenue_multiplier_to_app_full_info_obj(
    obj: Dict[str, Any],
    dict_publisher_id_cleaned_publisher_name: Dict[str, str],
    dict_publisher_id_revenue_multiplier: Dict[str, int],
) -> Dict[str, Any]:
    """
    Record-level version of add_cleaned_publisher_name_and_revenue_multiplier_to_app_full_info().

    If publisher_id in map:
      cleaned_publisher_name = map[publisher_id]
      revenue_multiplier = map[publisher_id]
    Else:
      cleaned_publisher_name = ""
      revenue_multiplier = 1
    """
    pid = obj.get("publisher_id")
    pid_s = str(pid).strip() if pid is not None else ""

    if pid_s in dict_publisher_id_cleaned_publisher_name:
        obj["cleaned_publisher_name"] = dict_publisher_id_cleaned_publisher_name[pid_s]
    else:
        obj["cleaned_publisher_name"] = ""

    if pid_s in dict_publisher_id_revenue_multiplier:
        obj["revenue_multiplier"] = dict_publisher_id_revenue_multiplier[pid_s]
    else:
        obj["revenue_multiplier"] = 1

    return obj


def adjust_cleaned_publisher_name_and_revenue_multiplier_of_app_full_info_special_cases_obj(
    obj: Dict[str, Any],
    dict_mapping_app_to_revenue_multiplier_special_case: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Record-level version of adjust_cleaned_publisher_name_and_revenue_multiplier_of_app_full_info_special_cases().

    If app_id in special-case map:
      cleaned_publisher_name = special["cleaned_publisher_name"]  (even if "")
      revenue_multiplier = special["revenue_multiplier"]          (only if not None)
    Else:
      keep current obj values
    """
    app_id = obj.get("app_id")
    app_id_s = str(app_id).strip() if app_id is not None else ""

    if app_id_s in dict_mapping_app_to_revenue_multiplier_special_case:
        special = dict_mapping_app_to_revenue_multiplier_special_case[app_id_s]
        obj["cleaned_publisher_name"] = special.get("cleaned_publisher_name", obj.get("cleaned_publisher_name"))

        rm = special.get("revenue_multiplier", None)
        if rm is not None:
            obj["revenue_multiplier"] = rm

    return obj


def adjust_ndjson_app_full_info(
    input_ndjson_path: str,
    output_ndjson_path: str,
    publisher_mapping_csv_path: str,
    special_case_csv_path: str,
    dedup_key: str = "app_id",
):
    """
    Streaming NDJSON -> NDJSON
      1) add cleaned_publisher_name + revenue_multiplier by publisher_id mapping
      2) apply special-case overrides by app_id
      3) drop duplicates by dedup_key (keep first)
      4) progress bar by bytes (works well for large files)

    Output: one JSON object per line (NDJSON).
    """
    pub_clean_map, pub_rm_map = build_publisher_maps(publisher_mapping_csv_path)
    special_map = build_special_case_map(special_case_csv_path)

    total_bytes = os.path.getsize(input_ndjson_path)
    bar = None
    if tqdm:
        bar = tqdm(total=total_bytes, unit="B", unit_scale=True, desc="Adjusting NDJSON")

    seen = set()
    written = 0
    skipped_dupes = 0

    with open(input_ndjson_path, "r", encoding="utf-8") as f_in, open(output_ndjson_path, "w", encoding="utf-8", newline="") as f_out:
        for line in f_in:
            if bar:
                bar.update(len(line.encode("utf-8", errors="ignore")))

            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)

            # dedup (like pandas drop_duplicates keep="first")
            k = obj.get(dedup_key)
            if k is not None:
                ks = str(k).strip()
                if ks in seen:
                    skipped_dupes += 1
                    continue
                seen.add(ks)

            # Apply your two steps
            obj = add_cleaned_publisher_name_and_revenue_multiplier_to_app_full_info_obj(
                obj, pub_clean_map, pub_rm_map
            )
            obj = adjust_cleaned_publisher_name_and_revenue_multiplier_of_app_full_info_special_cases_obj(
                obj, special_map
            )

            f_out.write(json.dumps(obj, ensure_ascii=False) + "\n")
            written += 1

    if bar:
        bar.close()

    print(f"Done. Output: {output_ndjson_path}")
    print(f"Written: {written:,} | Skipped duplicates: {skipped_dupes:,}")
    return {
        "output_path": output_ndjson_path,
        "records_written": written,
        "skipped_duplicates": skipped_dupes,
    }

from datetime import datetime, timedelta
import requests

def chunk_list(items, chunk_size):
    """Yield successive chunks from items."""
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]

def get_apps_active_users(
    api_key: str,
    base_url: str,
    ios_app_ids: list,
    android_app_ids: list,
    country_codes: str,
    start_date: str,
    end_date: str,
    max_periods: int = 1000,
    data_model: str = "DM_2025_Q2",
    app_chunk_size: int = 200,
    request_timeout: int = 60,
    show_progress: bool = True,
):
    """
    Fetch DAU/WAU/MAU (active users) for iOS + Android apps and return unified records.

    Output per record:
      app_id, country, date,
      dau_android, dau_iphone, dau_ipad,
      wau_android, wau_iphone, wau_ipad,
      mau_android, mau_iphone, mau_ipad
    """

    def split_date_range(start_date: str, end_date: str, time_period: str, max_periods: int):
        current_start = datetime.strptime(start_date, "%Y-%m-%d")
        final_end = datetime.strptime(end_date, "%Y-%m-%d")
        ranges = []

        if time_period == "day":
            step_days = max_periods
            while current_start <= final_end:
                current_end = min(current_start + timedelta(days=step_days - 1), final_end)
                ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
                current_start = current_end + timedelta(days=1)

        elif time_period == "week":
            while current_start <= final_end:
                current_end = current_start + timedelta(days=max_periods * 7 - 1)
                current_end = min(current_end, final_end)
                # align to Sunday (weeks begin Monday)
                current_end = current_end + timedelta(days=(6 - current_end.weekday()))
                current_end = min(current_end, final_end)

                ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
                current_start = current_end + timedelta(days=1)

        elif time_period == "month":
            while current_start <= final_end:
                current_end = current_start + timedelta(days=max_periods * 30 - 1)
                current_end = min(current_end, final_end)
                month_end = (current_end.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                current_end = min(month_end, final_end)

                ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
                current_start = (current_end.replace(day=1) + timedelta(days=32)).replace(day=1)

        else:
            raise ValueError("time_period must be one of: day, week, month")

        return ranges

    def fetch_one_os_timeperiod(os_name: str, app_ids: list, time_period: str):
        if not app_ids:
            return []

        endpoint = f"/v1/{os_name}/usage/active_users"
        url = f"{base_url}{endpoint}"
        headers = {"Authorization": f"Bearer {api_key}"}

        date_ranges = split_date_range(start_date, end_date, time_period, max_periods)

        # Precompute number of requests = date_ranges * app_chunks
        n_chunks = (len(app_ids) + app_chunk_size - 1) // app_chunk_size
        total_requests = len(date_ranges) * n_chunks

        desc = f"{os_name.upper()} {time_period.upper()} requests"
        pbar = tqdm(total=total_requests, desc=desc, disable=not show_progress)

        all_rows = []
        for dr_start, dr_end in date_ranges:
            for app_chunk in chunk_list(app_ids, app_chunk_size):
                params = {
                    "app_ids": ",".join(map(str, app_chunk)),
                    "time_period": time_period,
                    "start_date": dr_start,
                    "end_date": dr_end,
                    "countries": country_codes,
                    "data_model": data_model,
                }

                resp = requests.get(url, headers=headers, params=params, timeout=request_timeout)

                if resp.status_code != 200:
                    pbar.close()
                    raise RuntimeError(
                        f"SensorTower active_users failed: os={os_name}, time_period={time_period}, "
                        f"range={dr_start}..{dr_end}, status={resp.status_code}, body={resp.text[:500]}"
                    )

                payload = resp.json()
                if isinstance(payload, dict):
                    payload = payload.get("data", [])
                if not isinstance(payload, list):
                    pbar.close()
                    raise RuntimeError(f"Unexpected response format: {type(payload)}")

                all_rows.extend(payload)
                pbar.update(1)

        pbar.close()
        return all_rows

    # Top-level progress (6 blocks: iOS/Android × day/week/month)
    blocks = [
        ("ios", ios_app_ids, "day"),
        ("ios", ios_app_ids, "week"),
        ("ios", ios_app_ids, "month"),
        ("android", android_app_ids, "day"),
        ("android", android_app_ids, "week"),
        ("android", android_app_ids, "month"),
    ]

    block_pbar = tqdm(total=len(blocks), desc="Fetching blocks", disable=not show_progress)

    ios_day = fetch_one_os_timeperiod("ios", ios_app_ids, "day");     block_pbar.update(1)
    ios_week = fetch_one_os_timeperiod("ios", ios_app_ids, "week");   block_pbar.update(1)
    ios_month = fetch_one_os_timeperiod("ios", ios_app_ids, "month"); block_pbar.update(1)

    android_day = fetch_one_os_timeperiod("android", android_app_ids, "day");     block_pbar.update(1)
    android_week = fetch_one_os_timeperiod("android", android_app_ids, "week");   block_pbar.update(1)
    android_month = fetch_one_os_timeperiod("android", android_app_ids, "month"); block_pbar.update(1)

    block_pbar.close()

    # Merge SensorTower's DAU/WAU/MAU series only on their source dates.
    # Period-level semantics stay explicit for downstream aggregation.
    merged = {}

    def ensure_row(app_id, country, row_date):
        k = (str(app_id), str(country), str(row_date))
        if k not in merged:
            merged[k] = {
                "app_id": str(app_id),
                "country": country,
                "date": str(row_date),
                "dau_android": None,
                "dau_iphone": None,
                "dau_ipad": None,
                "wau_android": None,
                "wau_iphone": None,
                "wau_ipad": None,
                "mau_android": None,
                "mau_iphone": None,
                "mau_ipad": None,
            }
        return merged[k]

    def apply_ios(rows, prefix):
        for r in rows:
            row = ensure_row(r.get("app_id"), r.get("country"), r.get("date"))
            row[f"{prefix}_iphone"] = r.get("iphone_users")
            row[f"{prefix}_ipad"] = r.get("ipad_users")

    def apply_android(rows, prefix):
        for r in rows:
            row = ensure_row(r.get("app_id"), r.get("country"), r.get("date"))
            row[f"{prefix}_android"] = r.get("users")

    apply_ios(ios_day, "dau")
    apply_android(android_day, "dau")
    apply_ios(ios_week, "wau")
    apply_android(android_week, "wau")
    apply_ios(ios_month, "mau")
    apply_android(android_month, "mau")

    return list(merged.values())


def _split_active_users_date_range(start_date: str, end_date: str, time_period: str, max_periods: int):
    current_start = datetime.strptime(start_date, "%Y-%m-%d")
    final_end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []

    if time_period == "day":
        step_days = max_periods
        while current_start <= final_end:
            current_end = min(current_start + timedelta(days=step_days - 1), final_end)
            ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
            current_start = current_end + timedelta(days=1)

    elif time_period == "week":
        while current_start <= final_end:
            current_end = current_start + timedelta(days=max_periods * 7 - 1)
            current_end = min(current_end, final_end)
            current_end = current_end + timedelta(days=(6 - current_end.weekday()))
            current_end = min(current_end, final_end)

            ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
            current_start = current_end + timedelta(days=1)

    elif time_period == "month":
        while current_start <= final_end:
            current_end = current_start + timedelta(days=max_periods * 30 - 1)
            current_end = min(current_end, final_end)
            month_end = (current_end.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            current_end = min(month_end, final_end)

            ranges.append((current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")))
            current_start = (current_end.replace(day=1) + timedelta(days=32)).replace(day=1)

    else:
        raise ValueError("time_period must be one of: day, week, month")

    return ranges


def _count_non_empty_lines(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def _iter_ndjson_rows(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _fetch_active_users_block_to_cache(
    *,
    api_key: str,
    base_url: str,
    os_name: str,
    app_ids: list,
    time_period: str,
    country_codes: str,
    start_date: str,
    end_date: str,
    max_periods: int,
    data_model: str,
    app_chunk_size: int,
    request_timeout: int,
    show_progress: bool,
    cache_path: Path,
) -> dict[str, Any]:
    if cache_path.exists():
        row_count = _count_non_empty_lines(cache_path)
        print(f"Reusing cached active users block: {cache_path.name} ({row_count:,} rows)")
        return {
            "cache_path": str(cache_path),
            "row_count": row_count,
            "reused_cache": True,
        }

    if not app_ids:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text("", encoding="utf-8")
        return {
            "cache_path": str(cache_path),
            "row_count": 0,
            "reused_cache": False,
        }

    endpoint = f"/v1/{os_name}/usage/active_users"
    url = f"{base_url}{endpoint}"
    headers = {"Authorization": f"Bearer {api_key}"}
    date_ranges = _split_active_users_date_range(start_date, end_date, time_period, max_periods)
    n_chunks = (len(app_ids) + app_chunk_size - 1) // app_chunk_size
    total_requests = len(date_ranges) * n_chunks
    desc = f"{os_name.upper()} {time_period.upper()} requests"
    pbar = tqdm(total=total_requests, desc=desc, disable=not show_progress)
    tmp_path = cache_path.with_suffix(cache_path.suffix + ".partial")
    row_count = 0

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    if tmp_path.exists():
        tmp_path.unlink()

    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            for dr_start, dr_end in date_ranges:
                for app_chunk in chunk_list(app_ids, app_chunk_size):
                    params = {
                        "app_ids": ",".join(map(str, app_chunk)),
                        "time_period": time_period,
                        "start_date": dr_start,
                        "end_date": dr_end,
                        "countries": country_codes,
                        "data_model": data_model,
                    }

                    max_attempts = int(os.environ.get("ST_ACTIVE_USERS_REQUEST_RETRIES", "5"))
                    retry_sleep_seconds = float(os.environ.get("ST_ACTIVE_USERS_RETRY_SLEEP_SECONDS", "5"))
                    resp = None
                    for attempt in range(1, max_attempts + 1):
                        try:
                            resp = requests.get(url, headers=headers, params=params, timeout=request_timeout)
                            if resp.status_code == 200:
                                break
                            retryable_status = resp.status_code == 429 or resp.status_code >= 500
                            if not retryable_status or attempt == max_attempts:
                                raise RuntimeError(
                                    f"SensorTower active_users failed: os={os_name}, time_period={time_period}, "
                                    f"range={dr_start}..{dr_end}, status={resp.status_code}, body={resp.text[:500]}"
                                )
                            print(
                                "SensorTower active_users retrying: "
                                f"os={os_name}, time_period={time_period}, range={dr_start}..{dr_end}, "
                                f"status={resp.status_code}, attempt={attempt}/{max_attempts}"
                            )
                        except requests.RequestException as exc:
                            if attempt == max_attempts:
                                raise
                            print(
                                "SensorTower active_users retrying after request error: "
                                f"os={os_name}, time_period={time_period}, range={dr_start}..{dr_end}, "
                                f"attempt={attempt}/{max_attempts}, error={exc}"
                            )
                        sleep_for = retry_sleep_seconds * attempt + random.uniform(0, 1)
                        time.sleep(sleep_for)

                    if resp is None or resp.status_code != 200:
                        raise RuntimeError(
                            f"SensorTower active_users failed without a valid response: "
                            f"os={os_name}, time_period={time_period}, range={dr_start}..{dr_end}"
                        )

                    payload = resp.json()
                    if isinstance(payload, dict):
                        payload = payload.get("data", [])
                    if not isinstance(payload, list):
                        raise RuntimeError(f"Unexpected response format: {type(payload)}")

                    for row in payload:
                        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                        row_count += 1
                    pbar.update(1)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    finally:
        pbar.close()

    os.replace(tmp_path, cache_path)
    return {
        "cache_path": str(cache_path),
        "row_count": row_count,
        "reused_cache": False,
    }


def _export_active_users_from_cache_files(
    *,
    cache_dir: Path,
    export_path: Path,
) -> dict[str, Any]:
    scratch_root = Path(tempfile.gettempdir()) / "vn_strategy_active_users_merge"
    scratch_root.mkdir(parents=True, exist_ok=True)
    scratch_dir = scratch_root / export_path.stem
    if scratch_dir.exists():
        shutil.rmtree(scratch_dir, ignore_errors=True)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    db_path = scratch_dir / "active_users_merge.sqlite3"

    conn = sqlite3.connect(db_path)
    try:
        # This SQLite database is disposable scratch space built from durable
        # cache files, so favor throughput over crash recovery.
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA locking_mode=EXCLUSIVE")
        conn.execute("PRAGMA temp_store=FILE")
        conn.execute("PRAGMA cache_size=-200000")
        conn.execute(
            """
            CREATE TABLE active_users (
                app_id TEXT NOT NULL,
                country TEXT NOT NULL,
                date TEXT NOT NULL,
                dau_android INTEGER,
                dau_iphone INTEGER,
                dau_ipad INTEGER,
                wau_android INTEGER,
                wau_iphone INTEGER,
                wau_ipad INTEGER,
                mau_android INTEGER,
                mau_iphone INTEGER,
                mau_ipad INTEGER,
                PRIMARY KEY (app_id, country, date)
            )
            """
        )

        block_specs = [
            ("ios_day.ndjson", ("dau_iphone", "dau_ipad"), lambda row: (row.get("iphone_users"), row.get("ipad_users"))),
            ("android_day.ndjson", ("dau_android",), lambda row: (row.get("users"),)),
            ("ios_week.ndjson", ("wau_iphone", "wau_ipad"), lambda row: (row.get("iphone_users"), row.get("ipad_users"))),
            ("android_week.ndjson", ("wau_android",), lambda row: (row.get("users"),)),
            ("ios_month.ndjson", ("mau_iphone", "mau_ipad"), lambda row: (row.get("iphone_users"), row.get("ipad_users"))),
            ("android_month.ndjson", ("mau_android",), lambda row: (row.get("users"),)),
        ]

        for filename, columns, value_builder in block_specs:
            cache_path = cache_dir / filename
            if not cache_path.exists():
                continue
            print(f"Merging active users cache block: {filename}")
            update_clause = ", ".join(f"{column}=excluded.{column}" for column in columns)
            insert_columns = ["app_id", "country", "date", *columns]
            placeholders = ", ".join("?" for _ in insert_columns)
            sql = (
                f"INSERT INTO active_users ({', '.join(insert_columns)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT(app_id, country, date) DO UPDATE SET {update_clause}"
            )

            batch = []
            rows_seen = 0
            conn.execute("BEGIN")
            for row in _iter_ndjson_rows(cache_path):
                batch.append(
                    (
                        str(row.get("app_id")),
                        str(row.get("country")),
                        str(row.get("date"))[:10],
                        *value_builder(row),
                    )
                )
                rows_seen += 1
                if len(batch) >= 10000:
                    conn.executemany(sql, batch)
                    batch.clear()
                if rows_seen % 500000 == 0:
                    print(f"  {filename}: merged {rows_seen:,} source rows")
            if batch:
                conn.executemany(sql, batch)
            conn.commit()
            print(f"  {filename}: merged {rows_seen:,} source rows")

        row_count = int(conn.execute("SELECT COUNT(*) FROM active_users").fetchone()[0])
        tmp_output_path = export_path.with_suffix(export_path.suffix + ".partial")
        export_path.parent.mkdir(parents=True, exist_ok=True)
        if tmp_output_path.exists():
            tmp_output_path.unlink()

        with tmp_output_path.open("w", encoding="utf-8") as handle:
            handle.write("[")
            first = True
            cursor = conn.execute(
                """
                SELECT
                    app_id,
                    country,
                    date,
                    dau_android,
                    dau_iphone,
                    dau_ipad,
                    wau_android,
                    wau_iphone,
                    wau_ipad,
                    mau_android,
                    mau_iphone,
                    mau_ipad
                FROM active_users
                """
            )
            rows_written = 0
            for record in cursor:
                row = {
                    "app_id": record[0],
                    "country": record[1],
                    "date": record[2],
                    "dau_android": record[3],
                    "dau_iphone": record[4],
                    "dau_ipad": record[5],
                    "wau_android": record[6],
                    "wau_iphone": record[7],
                    "wau_ipad": record[8],
                    "mau_android": record[9],
                    "mau_iphone": record[10],
                    "mau_ipad": record[11],
                }
                if not first:
                    handle.write(",")
                first = False
                json.dump(row, handle, ensure_ascii=False)
                rows_written += 1
                if rows_written % 500000 == 0:
                    print(f"  final JSON: wrote {rows_written:,}/{row_count:,} rows")
            handle.write("]")
        print(f"  final JSON: wrote {row_count:,}/{row_count:,} rows")

        os.replace(tmp_output_path, export_path)
        return {
            "output_path": str(export_path),
            "row_count": row_count,
        }
    finally:
        conn.close()
        shutil.rmtree(scratch_dir, ignore_errors=True)


def create_table_app_active_users_checkpointed(
    api_key,
    base_url,
    str_start_date,
    str_end_date,
    df_app_full_info,
    json_export_path,
    country_codes="VN",
    max_periods=1000,
    data_model="DM_2025_Q2",
    export_path=None,
    app_chunk_size: int = 200,
    request_timeout: int = 60,
):
    if export_path is None:
        timestamp = int(time.time())
        country_label = country_codes.replace(",", "-").replace(" ", "")
        export_path = f"{json_export_path}/st_app_active_users_{country_label}_{str_start_date}_{str_end_date}_{timestamp}.json"

    export_path_obj = Path(export_path)
    cache_dir = export_path_obj.parent / f".{export_path_obj.stem}_active_users_tmp"
    cache_dir.mkdir(parents=True, exist_ok=True)

    list_app_ids_ios = list(
        map(lambda x: str(x), list(df_app_full_info[df_app_full_info["os"] == "ios"]["app_id"]))
    )
    list_app_ids_android = list(
        map(lambda x: str(x), list(df_app_full_info[df_app_full_info["os"] == "android"]["app_id"]))
    )

    print("Query app active users (DAU/WAU/MAU)...")
    print(f"Countries: {country_codes} | Start: {str_start_date} | End: {str_end_date}")
    print(f"iOS apps: {len(list_app_ids_ios):,} | Android apps: {len(list_app_ids_android):,}")

    block_pbar = tqdm(total=6, desc="Fetching blocks")
    block_specs = [
        ("ios", list_app_ids_ios, "day", cache_dir / "ios_day.ndjson"),
        ("ios", list_app_ids_ios, "week", cache_dir / "ios_week.ndjson"),
        ("ios", list_app_ids_ios, "month", cache_dir / "ios_month.ndjson"),
        ("android", list_app_ids_android, "day", cache_dir / "android_day.ndjson"),
        ("android", list_app_ids_android, "week", cache_dir / "android_week.ndjson"),
        ("android", list_app_ids_android, "month", cache_dir / "android_month.ndjson"),
    ]

    try:
        for os_name, app_ids, time_period, cache_path in block_specs:
            _fetch_active_users_block_to_cache(
                api_key=api_key,
                base_url=base_url,
                os_name=os_name,
                app_ids=app_ids,
                time_period=time_period,
                country_codes=country_codes,
                start_date=str_start_date,
                end_date=str_end_date,
                max_periods=max_periods,
                data_model=data_model,
                app_chunk_size=app_chunk_size,
                request_timeout=request_timeout,
                show_progress=True,
                cache_path=cache_path,
            )
            block_pbar.update(1)
    finally:
        block_pbar.close()

    print("Merging cached active users blocks into final JSON...")
    result = _export_active_users_from_cache_files(
        cache_dir=cache_dir,
        export_path=export_path_obj,
    )
    shutil.rmtree(cache_dir, ignore_errors=True)
    print(f"✅ Exported: {export_path_obj}")
    return result

def create_table_app_active_users(
    api_key,
    base_url,
    str_start_date,
    str_end_date,
    df_app_full_info,
    json_export_path,
    country_codes="VN",
    max_periods=1000,
    data_model="DM_2025_Q2",
    export_path=None,
):
    """
    Fetch DAU/WAU/MAU active users for iOS + Android apps in df_app_full_info,
    unified into rows with:
      app_id, country, date,
      dau_android, dau_iphone, dau_ipad,
      wau_android, wau_iphone, wau_ipad,
      mau_android, mau_iphone, mau_ipad

    Exports a timestamped JSON file to json_export_path and returns the list.
    """

    # iOS app IDs (as ints/strings both ok; we normalize to str for transport)
    list_app_ids_ios = list(
        map(lambda x: str(x),
            list(
                df_app_full_info[df_app_full_info["os"] == "ios"]["app_id"]
            )
        )
    )

    # Android package names
    list_app_ids_android = list(
        map(lambda x: str(x),
            list(
                df_app_full_info[df_app_full_info["os"] == "android"]["app_id"]
            )
        )
    )

    print("Query app active users (DAU/WAU/MAU)...")
    print(f"Countries: {country_codes} | Start: {str_start_date} | End: {str_end_date}")
    print(f"iOS apps: {len(list_app_ids_ios):,} | Android apps: {len(list_app_ids_android):,}")

    # Fetch unified records
    active_users_data = get_apps_active_users(
        api_key=api_key,
        base_url=base_url,
        ios_app_ids=list_app_ids_ios,
        android_app_ids=list_app_ids_android,
        country_codes=country_codes,
        start_date=str_start_date,
        end_date=str_end_date,
        max_periods=max_periods,
        data_model=data_model,
        show_progress=True,
    )

    # Export to JSON
    print("Exporting active users data to json...")
    if export_path is None:
        timestamp = int(time.time())
        country_label = country_codes.replace(",", "-").replace(" ", "")
        out_path = f"{json_export_path}/st_app_active_users_{country_label}_{str_start_date}_{str_end_date}_{timestamp}.json"
    else:
        out_path = export_path

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(active_users_data, f, ensure_ascii=False)

    print(f"✅ Exported: {out_path}")
    return active_users_data

def _add_months(dt: datetime, months: int) -> datetime:
    # Add months while keeping day in range
    y = dt.year + (dt.month - 1 + months) // 12
    m = (dt.month - 1 + months) % 12 + 1
    d = min(dt.day, [31,
                     29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28,
                     31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return dt.replace(year=y, month=m, day=d)

def _month_end(dt: datetime) -> datetime:
    first_next = _add_months(dt.replace(day=1), 1)
    return first_next - timedelta(days=1)

def split_month_ranges(start_date: str, end_date: str, max_months: int):
    """
    Split [start_date, end_date] into ranges of <= max_months months,
    aligned to month boundaries as much as possible.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    ranges = []
    cur = start

    while cur <= end:
        # tentative end = end of month after (max_months-1) months
        end_month_dt = _add_months(cur.replace(day=1), max_months - 1)
        cur_end = _month_end(end_month_dt)
        if cur_end > end:
            cur_end = end
        ranges.append((cur.strftime("%Y-%m-%d"), cur_end.strftime("%Y-%m-%d")))
        cur = cur_end + timedelta(days=1)

    return ranges

def get_apps_retention_daily(
    api_key: str,
    base_url: str,
    app_ids: list,
    region_codes: str,
    start_date: str,
    end_date: str,
    *,
    max_months_per_request: int = 6,
    app_chunk_size: int = 200,
    request_timeout: int = 60,
    show_progress: bool = True,
):
    """
    Fetch daily retention metrics (D1, D2..D7, D14, D30, D60, D90, D180, D365)
    for a set of app_ids, within region_codes and date window.

    Returns: list[dict] with fields:
      app_id, country, date,
      est_retention_d1, est_retention_d2, ... est_retention_d365
    """

    if not app_ids:
        return []

    # Normalize regions like "VN, TH" -> ["VN","TH"]
    regions = [r.strip() for r in region_codes.split(",") if r.strip()]
    if not regions:
        raise ValueError("region_codes must contain at least one region code, e.g. 'VN' or 'VN,TH'.")

    url = f"{base_url}/v1/facets/metrics"

    # Split the date window into month-aligned chunks
    date_ranges = split_month_ranges(start_date, end_date, max_months_per_request)

    # Estimate total requests for progress bar
    num_app_chunks = (len(app_ids) + app_chunk_size - 1) // app_chunk_size
    total_requests = len(regions) * len(date_ranges) * num_app_chunks

    pbar = tqdm(total=total_requests, desc="Retention daily requests", disable=not show_progress)

    results = []

    for region in regions:
        for dr_start, dr_end in date_ranges:
            for app_chunk in chunk_list(app_ids, app_chunk_size):
                try:
                    for row in _iter_retention_daily_rows_for_chunk(
                        url=url,
                        api_key=api_key,
                        region=region,
                        dr_start=dr_start,
                        dr_end=dr_end,
                        app_chunk=app_chunk,
                        request_timeout=request_timeout,
                        pbar=pbar,
                    ):
                        results.append(row)
                except Exception:
                    pbar.close()
                    raise

    pbar.close()
    return results

def _iter_retention_daily_rows_for_chunk(
    *,
    url: str,
    api_key: str,
    region: str,
    dr_start: str,
    dr_end: str,
    app_chunk: list,
    request_timeout: int,
    pbar,
):
    params = {
        "retention": "",  # flag-style param (?retention)
        "bundle": "retention_daily",
        "breakdown": "date,app_id",
        "start_date": dr_start,
        "end_date": dr_end,
        "regions": region,
        "app_ids": ",".join(map(str, app_chunk)),
        "auth_token": api_key,
    }

    resp = _get_with_retry(
        url,
        params=params,
        timeout=request_timeout,
        max_retries=6,
        base_sleep=1.5,
        max_sleep=60,
    )

    if resp.status_code == 414 and len(app_chunk) > 1:
        midpoint = max(1, len(app_chunk) // 2)
        left_chunk = app_chunk[:midpoint]
        right_chunk = app_chunk[midpoint:]
        print(
            f"Retention request URI too large for {len(app_chunk):,} apps; "
            f"retrying as chunks of {len(left_chunk):,} and {len(right_chunk):,}."
        )
        if getattr(pbar, "total", None) is not None:
            pbar.total += 1
            pbar.refresh()
        yield from _iter_retention_daily_rows_for_chunk(
            url=url,
            api_key=api_key,
            region=region,
            dr_start=dr_start,
            dr_end=dr_end,
            app_chunk=left_chunk,
            request_timeout=request_timeout,
            pbar=pbar,
        )
        yield from _iter_retention_daily_rows_for_chunk(
            url=url,
            api_key=api_key,
            region=region,
            dr_start=dr_start,
            dr_end=dr_end,
            app_chunk=right_chunk,
            request_timeout=request_timeout,
            pbar=pbar,
        )
        return

    if resp.status_code != 200:
        raise RuntimeError(
            f"Retention API failed: region={region}, range={dr_start}..{dr_end}, "
            f"status={resp.status_code}, body={resp.text[:800]}"
        )

    payload = resp.json()
    data = payload.get("data", []) if isinstance(payload, dict) else payload
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected response format: {type(data)}")

    for row in data:
        out = {
            "app_id": str(row.get("app_id")) if row.get("app_id") is not None else None,
            "country": region,
            "date": row.get("date"),
        }
        for k in RETENTION_FIELDS_DAILY:
            out[k] = row.get(k)
        yield out

    pbar.update(1)

def iter_apps_retention_daily(
    api_key: str,
    base_url: str,
    app_ids: list,
    region_codes: str,
    start_date: str,
    end_date: str,
    *,
    max_months_per_request: int = 6,
    app_chunk_size: int = 200,
    request_timeout: int = 120,
    show_progress: bool = True,
):
    """
    Stream daily retention rows (generator). Does NOT accumulate all results in memory.
    Yields dicts with:
      app_id, country, date, est_retention_d1..d365
    """
    if not app_ids:
        return
        yield  # make generator happy

    regions = [r.strip() for r in region_codes.split(",") if r.strip()]
    if not regions:
        raise ValueError("region_codes must contain at least one region code, e.g. 'VN' or 'VN,TH'.")

    url = f"{base_url}/v1/facets/metrics"
    date_ranges = split_month_ranges(start_date, end_date, max_months_per_request)

    num_app_chunks = (len(app_ids) + app_chunk_size - 1) // app_chunk_size
    total_requests = len(regions) * len(date_ranges) * num_app_chunks
    pbar = tqdm(total=total_requests, desc="Retention daily requests", disable=not show_progress)

    for region in regions:
        for dr_start, dr_end in date_ranges:
            for app_chunk in chunk_list(app_ids, app_chunk_size):
                try:
                    yield from _iter_retention_daily_rows_for_chunk(
                        url=url,
                        api_key=api_key,
                        region=region,
                        dr_start=dr_start,
                        dr_end=dr_end,
                        app_chunk=app_chunk,
                        request_timeout=request_timeout,
                        pbar=pbar,
                    )
                except Exception:
                    pbar.close()
                    raise

    pbar.close()

def create_table_app_retention_daily_ndjson(
    api_key,
    base_url,
    str_start_date,
    str_end_date,
    df_app_full_info,
    ndjson_export_path,
    region_codes="VN",
    max_months_per_request=6,
    app_chunk_size=200,
    show_progress=True,
    export_path=None,
):
    """
    Streaming NDJSON export for daily retention.
    - Does NOT build/return a giant in-memory list.
    - Writes one JSON object per line to an .ndjson file.

    Returns: (out_path, record_count)
    """

    # iOS app_ids
    list_app_ids_ios = list(
        map(lambda x: str(x),
            list(df_app_full_info[df_app_full_info["os"] == "ios"]["app_id"])
        )
    )

    # Android bundle ids
    list_app_ids_android = list(
        map(lambda x: str(x),
            list(df_app_full_info[df_app_full_info["os"] == "android"]["app_id"])
        )
    )

    # Combine + dedupe
    app_ids = sorted(set(list_app_ids_ios + list_app_ids_android))

    print("Query retention daily metrics (streaming to NDJSON)...")
    print(f"Regions: {region_codes} | Start: {str_start_date} | End: {str_end_date}")
    print(f"Total apps: {len(app_ids):,} (iOS={len(list_app_ids_ios):,}, Android={len(list_app_ids_android):,})")

    # Output path
    if export_path is None:
        timestamp = int(time.time())
        region_label = region_codes.replace(",", "-").replace(" ", "")
        out_path = (
            f"{ndjson_export_path}/st_app_retention_daily_{region_label}_"
            f"{str_start_date}_{str_end_date}_{timestamp}.ndjson"
        )
    else:
        out_path = export_path

    record_count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for rec in iter_apps_retention_daily(
            api_key=api_key,
            base_url=base_url,
            app_ids=app_ids,
            region_codes=region_codes,
            start_date=str_start_date,
            end_date=str_end_date,
            max_months_per_request=max_months_per_request,
            app_chunk_size=app_chunk_size,
            show_progress=show_progress,
        ):
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            record_count += 1

    print(f"✅ Exported: {out_path}")
    print(f"✅ Records written: {record_count:,}")
    return out_path, record_count

def get_app_reviews(
    api_key: str,
    base_url: str,
    os_name: str,
    app_ids: List[Any],
    country_codes: str,
    start_date: str,
    end_date: str,
    *,
    limit: int = 200,                 # max 200 per docs
    page_start: int = 1,
    request_timeout: TimeoutType = (10, 60),
    show_progress: bool = True,
    verbose: bool = False,            # set True for page-by-page prints
    max_pages_per_app_country: Optional[int] = None,  # for sampling/debug
    ios_use_countries_param: bool = True,             # True: countries=VN,TH; False: loop like android
) -> List[Dict[str, Any]]:
    """
    Fetch detailed user reviews for apps.

    Output fields:
      app_id, country, date, review_id, username, rating, title, version, tags,
      content, sentiment, permalink, store_language

    Behavior:
    - Progress bar updates PER PAGE request (so it won't look frozen).
    - For iOS:
        - Default: one run per app with `countries=VN,TH,...` (fewer calls)
        - Optional: set ios_use_countries_param=False to call per country like android
    - For Android:
        - Calls per (app_id, country) using `country=VN`
    """
    os_name = os_name.lower().strip()
    if os_name not in ("ios", "android"):
        raise ValueError("os_name must be 'ios' or 'android'")

    countries = [c.strip() for c in country_codes.split(",") if c.strip()]
    if not countries:
        raise ValueError("country_codes must contain at least one country code, e.g. 'VN' or 'VN,TH'")

    if limit > 200 or limit <= 0:
        raise ValueError("limit must be between 1 and 200")

    url = f"{base_url}/v1/{os_name}/review/get_reviews"
    results: List[Dict[str, Any]] = []

    # Per-page progress bar (unknown total)
    pbar = tqdm(
        total=None,
        desc=f"Fetching {os_name} reviews (pages)",
        disable=not show_progress,
        mininterval=0.2,
    )

    def _emit_reviews(feedback: list, fallback_country: Optional[str]):
        for r in feedback:
            results.append({
                "app_id": str(r.get("app_id", "")),
                "country": r.get("country") or fallback_country,
                "date": r.get("date"),
                "review_id": r.get("id"),
                "username": r.get("username"),
                "rating": r.get("rating"),
                "title": r.get("title"),
                "version": r.get("version"),
                "tags": r.get("tags") if isinstance(r.get("tags"), list) else [],
                "content": r.get("content"),
                "sentiment": r.get("sentiment"),
                "permalink": r.get("permalink"),
                "store_language": r.get("store_language"),
            })

    for app_id in app_ids:
        app_id_str = str(app_id)

        if os_name == "ios" and ios_use_countries_param:
            # Single call stream for all selected iOS countries
            batches = [("countries", ",".join(countries))]
        else:
            # Per-country calls (Android always, and optionally iOS)
            batches = [("country", c) for c in countries]

        for mode, country_val in batches:
            page = page_start
            page_count: Optional[int] = None
            pages_fetched = 0

            while True:
                params: Dict[str, Any] = {
                    "app_id": app_id_str,
                    "start_date": start_date,
                    "end_date": end_date,
                    "limit": limit,
                    "page": page,
                    "auth_token": api_key,
                }
                params[mode] = country_val

                # heartbeat in tqdm
                pbar.set_postfix(app=app_id_str, c=country_val, page=page, pages=page_count)

                if verbose:
                    print(f"[{os_name}] app={app_id_str} {mode}={country_val} page={page} ...")

                resp = _get_with_retry(url, params=params, timeout=request_timeout)

                if resp.status_code != 200:
                    raise RuntimeError(
                        f"Review API failed: os={os_name}, app_id={app_id_str}, {mode}={country_val}, "
                        f"page={page}, status={resp.status_code}, body={resp.text[:800]}"
                    )

                payload = resp.json() if resp.content else {}
                if not isinstance(payload, dict):
                    raise RuntimeError(f"Unexpected response format: {type(payload)}")

                feedback = payload.get("feedback", [])
                if not isinstance(feedback, list):
                    raise RuntimeError(f"Unexpected feedback format: {type(feedback)}")

                if page_count is None:
                    pc = payload.get("page_count")
                    try:
                        page_count = int(pc) if pc is not None else None
                    except Exception:
                        page_count = None

                # Fallback country when API doesn't provide it (should usually provide)
                fallback_country = None
                if mode == "country":
                    fallback_country = country_val
                elif mode == "countries" and len(countries) == 1:
                    fallback_country = countries[0]

                _emit_reviews(feedback, fallback_country)

                if verbose:
                    got = len(feedback)
                    if page_count is not None:
                        print(f"  -> got {got} reviews (page {page}/{page_count})")
                    else:
                        print(f"  -> got {got} reviews (page {page})")

                # progress updates per request/page
                pbar.update(1)
                pages_fetched += 1

                # Stop conditions
                if max_pages_per_app_country is not None and pages_fetched >= max_pages_per_app_country:
                    break

                if page_count is not None:
                    if page >= page_count:
                        break
                else:
                    # If no page_count, stop when short page or empty
                    if not feedback or len(feedback) < limit:
                        break

                page += 1

    pbar.close()
    return results

def iter_app_reviews(
    api_key: str,
    base_url: str,
    os_name: str,
    app_ids: List[Any],
    country_codes: str,
    start_date: str,
    end_date: str,
    *,
    limit: int = 200,
    page_start: int = 1,
    request_timeout: Any = (10, 60),
    show_progress: bool = True,
    verbose: bool = False,
    ios_use_countries_param: bool = True,
) -> Iterable[Dict[str, Any]]:
    """
    STREAM reviews page-by-page (generator). Does NOT store everything in memory.
    Yields dicts with fields:
      app_id, country, date, review_id, username, rating, title, version,
      tags, content, sentiment, permalink, store_language
    """
    os_name = os_name.lower().strip()
    if os_name not in ("ios", "android"):
        raise ValueError("os_name must be 'ios' or 'android'")

    countries = [c.strip() for c in country_codes.split(",") if c.strip()]
    if not countries:
        raise ValueError("country_codes must contain at least one country code, e.g. 'VN' or 'VN,TH'")

    if limit <= 0 or limit > 200:
        raise ValueError("limit must be between 1 and 200")

    url = f"{base_url}/v1/{os_name}/review/get_reviews"

    # Per-page progress (unknown total)
    pbar = tqdm(
        total=None,
        desc=f"Fetching {os_name} reviews (pages)",
        disable=not show_progress,
        mininterval=0.2,
    )

    for app_id in app_ids:
        app_id_str = str(app_id)

        if os_name == "ios" and ios_use_countries_param:
            batches = [("countries", ",".join(countries))]
        else:
            batches = [("country", c) for c in countries]

        for mode, country_val in batches:
            page = page_start
            page_count: Optional[int] = None

            while True:
                params: Dict[str, Any] = {
                    "app_id": app_id_str,
                    "start_date": start_date,
                    "end_date": end_date,
                    "limit": limit,
                    "page": page,
                    "auth_token": api_key,
                }
                params[mode] = country_val

                pbar.set_postfix(app=app_id_str, c=country_val, page=page, pages=page_count)

                if verbose:
                    print(f"[{os_name}] app={app_id_str} {mode}={country_val} page={page} ...")

                resp = _get_with_retry(url, params=params, timeout=request_timeout)

                if resp.status_code != 200:
                    pbar.close()
                    raise RuntimeError(
                        f"Review API failed: os={os_name}, app_id={app_id_str}, {mode}={country_val}, "
                        f"page={page}, status={resp.status_code}, body={resp.text[:800]}"
                    )

                payload = resp.json() if resp.content else {}
                if not isinstance(payload, dict):
                    pbar.close()
                    raise RuntimeError(f"Unexpected response format: {type(payload)}")

                feedback = payload.get("feedback", [])
                if not isinstance(feedback, list):
                    pbar.close()
                    raise RuntimeError(f"Unexpected feedback format: {type(feedback)}")

                if page_count is None:
                    pc = payload.get("page_count")
                    try:
                        page_count = int(pc) if pc is not None else None
                    except Exception:
                        page_count = None

                fallback_country = country_val if mode == "country" else (countries[0] if len(countries) == 1 else None)

                for r in feedback:
                    yield {
                        "app_id": str(r.get("app_id", app_id_str)),
                        "country": r.get("country") or fallback_country,
                        "date": r.get("date"),
                        "review_id": r.get("id"),
                        "username": r.get("username"),
                        "rating": r.get("rating"),
                        "title": r.get("title"),
                        "version": r.get("version"),
                        "tags": r.get("tags") if isinstance(r.get("tags"), list) else [],
                        "content": r.get("content"),
                        "sentiment": r.get("sentiment"),
                        "permalink": r.get("permalink"),
                        "store_language": r.get("store_language"),
                    }

                pbar.update(1)

                # Stop conditions
                if page_count is not None:
                    if page >= page_count:
                        break
                else:
                    if not feedback or len(feedback) < limit:
                        break

                page += 1

    pbar.close()

def create_table_app_user_reviews_ndjson_streaming(
    api_key: str,
    base_url: str,
    str_start_date: str,
    str_end_date: str,
    df_app_full_info,
    ndjson_export_path: str,
    *,
    country_codes: str = "VN",
    limit: int = 200,
    request_timeout: Any = (10, 60),
    show_progress: bool = True,
    verbose: bool = False,
    export_path: Optional[str] = None,
) -> Tuple[str, int]:
    """
    BIG-DATA wrapper: streams reviews and writes NDJSON line-by-line.
    Does NOT keep reviews in memory.

    Returns: (out_path, record_count)
    """

    ios_app_ids: List[str] = list(
        map(lambda x: str(x),
            list(df_app_full_info[df_app_full_info["os"] == "ios"]["app_id"])
        )
    )
    android_app_ids: List[str] = list(
        map(lambda x: str(x),
            list(df_app_full_info[df_app_full_info["os"] == "android"]["app_id"])
        )
    )

    print("Fetching user reviews (STREAMING -> NDJSON)...")
    print(f"Countries: {country_codes} | Start: {str_start_date} | End: {str_end_date}")
    print(f"Apps: iOS={len(ios_app_ids):,} | Android={len(android_app_ids):,}")
    print("Tip: This can be very large (millions of reviews). NDJSON is the right choice.")

    if export_path is None:
        timestamp = int(time.time())
        country_label = country_codes.replace(",", "-").replace(" ", "")
        out_path = (
            f"{ndjson_export_path}/st_app_user_reviews_{country_label}_"
            f"{str_start_date}_{str_end_date}_{timestamp}.ndjson"
        )
    else:
        out_path = export_path

    record_count = 0

    with open(out_path, "w", encoding="utf-8") as f:
        # Android
        if android_app_ids:
            for rec in iter_app_reviews(
                api_key=api_key,
                base_url=base_url,
                os_name="android",
                app_ids=android_app_ids,
                country_codes=country_codes,
                start_date=str_start_date,
                end_date=str_end_date,
                limit=limit,
                request_timeout=request_timeout,
                show_progress=show_progress,
                verbose=verbose,
            ):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                record_count += 1

        # iOS
        if ios_app_ids:
            for rec in iter_app_reviews(
                api_key=api_key,
                base_url=base_url,
                os_name="ios",
                app_ids=ios_app_ids,
                country_codes=country_codes,
                start_date=str_start_date,
                end_date=str_end_date,
                limit=limit,
                request_timeout=request_timeout,
                show_progress=show_progress,
                verbose=verbose,
                ios_use_countries_param=True,   # iOS supports countries=VN,TH,...
            ):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                record_count += 1

    print(f"✅ Exported: {out_path}")
    print(f"✅ Records written: {record_count:,}")
    return out_path, record_count

def get_apps_demographics_all_time(
    *,
    api_key: str,
    base_url: str,
    os_name: str,  # "ios" or "android"
    app_ids: Sequence[Union[str, int]],
    country_code: str = "SE_ASIA",
    # all_time ignores dates, but API still requires them -> send safe wide range
    start_date: str = "2015-10-01",
    end_date: Optional[str] = None,  # defaults to yesterday
    date_granularity: str = "all_time",  # keep default as required by your case
    app_chunk_size: int = 200,
    timeout: float = 60.0,
    show_progress: bool = True,
) -> List[Dict[str, Any]]:
    """
    Fetch Sensor Tower demographics (usage/demographics) for many apps.

    Output fields (per row):
      app_id, country, date, date_granularity, confidence, average_age_total, female, male,
      normalized_demographics_female_18, normalized_demographics_female_25, normalized_demographics_female_35,
      normalized_demographics_female_45, normalized_demographics_female_55,
      normalized_demographics_male_18, normalized_demographics_male_25, normalized_demographics_male_35,
      normalized_demographics_male_45, normalized_demographics_male_55
    """
    os_name = str(os_name).strip().lower()
    if os_name not in {"ios", "android"}:
        raise ValueError("os_name must be 'ios' or 'android'")

    if end_date is None:
        end_date = (date.today().fromordinal(date.today().toordinal() - 1)).isoformat()

    url = f"{base_url}/v1/{os_name}/usage/demographics"

    # Normalize app ids to strings for the API
    app_ids_str = [str(x) for x in app_ids if str(x).strip() != ""]
    chunks = chunk_list(app_ids_str, app_chunk_size)

    fetched_at = date.today().isoformat()

    out_rows: List[Dict[str, Any]] = []

    iterator = chunks
    pbar = None
    if show_progress:
        pbar = tqdm(chunks, desc=f"Fetching {os_name} demographics", unit="chunk")
        iterator = pbar

    for chunk in iterator:
        for row in _iter_demographics_rows_for_chunk(
            url=url,
            api_key=api_key,
            country_code=country_code,
            date_granularity=date_granularity,
            start_date=start_date,
            end_date=end_date,
            app_chunk=chunk,
            timeout=timeout,
            fetched_at=fetched_at,
            pbar=pbar,
        ):
            out_rows.append(row)

    return out_rows

def _iter_demographics_rows_for_chunk(
    *,
    url: str,
    api_key: str,
    country_code: str,
    date_granularity: str,
    start_date: str,
    end_date: str,
    app_chunk: Sequence[str],
    timeout: Any,
    fetched_at: str,
    pbar,
) -> Iterable[Dict[str, Any]]:
    params = {
        "auth_token": api_key,
        "app_ids": ",".join(app_chunk),
        "country": country_code,
        "date_granularity": date_granularity,
        "start_date": start_date,
        "end_date": end_date,
    }

    resp = _get_with_retry(url, params=params, timeout=timeout)
    if resp.status_code == 414 and len(app_chunk) > 1:
        midpoint = max(1, len(app_chunk) // 2)
        left_chunk = app_chunk[:midpoint]
        right_chunk = app_chunk[midpoint:]
        print(
            f"Demographics request URI too large for {len(app_chunk):,} apps; "
            f"retrying as chunks of {len(left_chunk):,} and {len(right_chunk):,}."
        )
        if pbar is not None and getattr(pbar, "total", None) is not None:
            pbar.total += 1
            pbar.refresh()
        yield from _iter_demographics_rows_for_chunk(
            url=url,
            api_key=api_key,
            country_code=country_code,
            date_granularity=date_granularity,
            start_date=start_date,
            end_date=end_date,
            app_chunk=left_chunk,
            timeout=timeout,
            fetched_at=fetched_at,
            pbar=pbar,
        )
        yield from _iter_demographics_rows_for_chunk(
            url=url,
            api_key=api_key,
            country_code=country_code,
            date_granularity=date_granularity,
            start_date=start_date,
            end_date=end_date,
            app_chunk=right_chunk,
            timeout=timeout,
            fetched_at=fetched_at,
            pbar=pbar,
        )
        return

    if resp.status_code != 200:
        raise RuntimeError(
            f"Demographics request failed: {resp.status_code} {resp.text[:1000]}"
        )

    payload = resp.json() or {}
    app_data = payload.get("app_data") or []

    for item in app_data:
        nd = item.get("normalized_demographics") or {}
        yield {
            "app_id": item.get("app_id"),
            "country": item.get("country", country_code),
            "date": fetched_at,
            "date_granularity": item.get("date_granularity", date_granularity),
            "confidence": item.get("confidence"),
            "average_age_total": item.get("average_age_total"),
            "female": item.get("female"),
            "male": item.get("male"),
            "normalized_demographics_female_18": nd.get("female_18"),
            "normalized_demographics_female_25": nd.get("female_25"),
            "normalized_demographics_female_35": nd.get("female_35"),
            "normalized_demographics_female_45": nd.get("female_45"),
            "normalized_demographics_female_55": nd.get("female_55"),
            "normalized_demographics_male_18": nd.get("male_18"),
            "normalized_demographics_male_25": nd.get("male_25"),
            "normalized_demographics_male_35": nd.get("male_35"),
            "normalized_demographics_male_45": nd.get("male_45"),
            "normalized_demographics_male_55": nd.get("male_55"),
        }

    if pbar is not None:
        pbar.update(1)

def iter_apps_demographics_all_time(
    *,
    api_key: str,
    base_url: str,
    os_name: str,  # "ios" or "android"
    app_ids: Sequence[Union[str, int]],
    country_code: str = "SE_ASIA",
    start_date: str = "2020-01-01",   # required by API even for all_time
    end_date: Optional[str] = None,
    date_granularity: str = "all_time",
    app_chunk_size: int = 200,
    timeout: Any = 60,
    show_progress: bool = True,
) -> Iterable[Dict[str, Any]]:
    """
    Stream Sensor Tower demographics rows for many apps (generator).
    Requires your existing _get_with_retry(url, params=..., timeout=...).

    Yields dicts with fields:
      app_id, country, date, date_granularity, confidence, average_age_total, female, male,
      normalized_demographics_female_18..55, normalized_demographics_male_18..55
    """
    os_name = str(os_name).strip().lower()
    if os_name not in {"ios", "android"}:
        raise ValueError("os_name must be 'ios' or 'android'")

    if end_date is None:
        # yesterday (safe default)
        end_date = (date.today().fromordinal(date.today().toordinal() - 1)).isoformat()

    url = f"{base_url}/v1/{os_name}/usage/demographics"

    app_ids_str = [str(x) for x in app_ids if str(x).strip() != ""]
    chunks = [app_ids_str[i:i + app_chunk_size] for i in range(0, len(app_ids_str), app_chunk_size)]

    fetched_at = date.today().isoformat()

    iterator = chunks
    pbar = None
    if show_progress:
        pbar = tqdm(chunks, desc=f"Fetching {os_name} demographics", unit="chunk")
        iterator = pbar

    for chunk in iterator:
        yield from _iter_demographics_rows_for_chunk(
            url=url,
            api_key=api_key,
            country_code=country_code,
            date_granularity=date_granularity,
            start_date=start_date,
            end_date=end_date,
            app_chunk=chunk,
            timeout=timeout,
            fetched_at=fetched_at,
            pbar=pbar,
        )


def create_table_app_demographics_all_time_ndjson_streaming(
    api_key: str,
    base_url: str,
    df_app_full_info,
    ndjson_export_path: str,
    *,
    country_code: str = "SE_ASIA",
    start_date: str = "2020-01-01",      # required even for all_time
    end_date: Optional[str] = None,
    date_granularity: str = "all_time",
    app_chunk_size: int = 200,
    timeout: Any = 60,
    show_progress: bool = True,
    export_path: Optional[str] = None,
) -> Tuple[str, int]:
    """
    Wrapper like retention/active-users:
    - Uses df_app_full_info to get iOS + Android app_ids
    - Streams API results directly to NDJSON (no giant in-memory list)
    - Returns (out_path, record_count)
    """

    ios_app_ids: List[str] = list(
        map(lambda x: str(x),
            list(df_app_full_info[df_app_full_info["os"] == "ios"]["app_id"])
        )
    )
    android_app_ids: List[str] = list(
        map(lambda x: str(x),
            list(df_app_full_info[df_app_full_info["os"] == "android"]["app_id"])
        )
    )

    print("Fetching demographics (ALL_TIME) -> NDJSON (streaming)...")
    print(f"Country: {country_code} | date_granularity={date_granularity}")
    print(f"Apps: iOS={len(ios_app_ids):,} | Android={len(android_app_ids):,}")

    if export_path is None:
        ts = int(time.time())
        out_path = (
            f"{ndjson_export_path}/st_app_demographics_{country_code}_"
            f"{date_granularity}_{ts}.ndjson"
        )
    else:
        out_path = export_path

    n = 0
    with open(out_path, "w", encoding="utf-8") as f:
        # Android
        if android_app_ids:
            for rec in iter_apps_demographics_all_time(
                api_key=api_key,
                base_url=base_url,
                os_name="android",
                app_ids=android_app_ids,
                country_code=country_code,
                start_date=start_date,
                end_date=end_date,
                date_granularity=date_granularity,
                app_chunk_size=app_chunk_size,
                timeout=timeout,
                show_progress=show_progress,
            ):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n += 1

        # iOS
        if ios_app_ids:
            for rec in iter_apps_demographics_all_time(
                api_key=api_key,
                base_url=base_url,
                os_name="ios",
                app_ids=ios_app_ids,
                country_code=country_code,
                start_date=start_date,
                end_date=end_date,
                date_granularity=date_granularity,
                app_chunk_size=app_chunk_size,
                timeout=timeout,
                show_progress=show_progress,
            ):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n += 1

    print(f"✅ Exported: {out_path}")
    print(f"✅ Records written: {n:,}")
    return out_path, n
