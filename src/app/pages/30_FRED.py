import datetime
import os, sys
import random
import sqlite3
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.utils.pandas_utils import df_filter, set_cols_numeric
import src.config as cfg

# ---------------------------------------- #
# FRED Release Data
# ---------------------------------------- #

import requests

st.title("FRED Data Series Viewer")

from collections import namedtuple
FREDRelease = namedtuple("FREDRelease", ["id", "name", "link", "press_release", "notes",
                                         "realtime_start", "realtime_end"])
FREDSeries = namedtuple("FREDSeries", ['id', 'title', 'realtime_start', 'realtime_end',
                                       'observation_start', 'observation_end',
                                       'frequency', 'frequency_short',
                                       'units', 'units_short',
                                       'seasonal_adjustment', 'seasonal_adjustment_short',
                                       'last_updated', 'popularity', 'group_popularity','notes'])


st.write({
  "id": "ABS10Y",
  "realtime_start": "2023-11-11",
  "realtime_end": "2023-11-11",
  "title": "Asset-backed securities held by TALF LLC (Face value): Maturing in over 10 years (DISCONTINUED)",
  "observation_start": "2002-12-18",
  "observation_end": "2014-11-05",
  "frequency": "Weekly, As of Wednesday",
  "frequency_short": "W",
  "units": "Millions of Dollars",
  "units_short": "Mil. of $",
  "seasonal_adjustment": "Not Seasonally Adjusted",
  "seasonal_adjustment_short": "NSA",
  "last_updated": "2014-11-06 15:54:24-06",
  "popularity": 1,
  "group_popularity": 1
}.keys())


class FREDSeriesAPI:
    def __init__(self):
        self.api_key = cfg.fred_api_key
        self.base_url = "https://api.stlouisfed.org/fred/series"

    def get_series_info(self, series_id, file_type="json"):
        """
        Get information about a specific series from the FRED API.

        Args:
            series_id (str): The series ID to retrieve information for.
            file_type (str, optional): The file type of the response, either 'json' or 'xml'. Defaults to 'json'.

        Returns:
            dict: The information about the series.
        """
        if series_id is None:
            return None
        endpoint = f"{self.base_url}"
        params = {
            "api_key": self.api_key,
            "series_id": series_id,
            "file_type": file_type
        }
        params = {k: v for k, v in params.items() if v is not None}
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json()


class FREDReleasesAPI:

    def __init__(self):
        self.api_key = cfg.fred_api_key
        self.fred_root_url = "https://api.stlouisfed.org"
        self.fred_releases_all = dict()
        self.release_names_all = []
        self.fred_release_series = dict()
        self.fred_release_series_attributes = dict()
        self.get_all_releases()

    def get_all_releases(self):
        response_data = self.request_all_releases()
        default_fields = {k: None for k in FREDRelease._fields}
        for r in response_data:
            self.fred_releases_all[r.get("name")] = FREDRelease(**{**default_fields, **r})
            self.release_names_all.append(r.get("name"))
        self.release_names_all.sort()

    def get_release_series(self, release_name):
        if release_name is None:
            return
        release_id = self.fred_releases_all.get(release_name).id
        series_response = self.request_release_series(release_id)
        default_series = {k: None for k in FREDSeries._fields}
        self.fred_release_series[release_name] = dict()
        self.fred_release_series_attributes[release_name] = {"units": set(), "frequency": set(), "sa": set()}
        for s in series_response:
            self.fred_release_series[release_name][s.get("id")] = FREDSeries(**{**default_series, **s})
            self.fred_release_series_attributes[release_name]['units'].add(s.get("units"))
            self.fred_release_series_attributes[release_name]['frequency'].add(s.get("frequency"))
            self.fred_release_series_attributes[release_name]['sa'].add(s.get("seasonal_adjustment_short"))
            self.fred_release_series_attributes[release_name]['last_updated'] = s.get("last_updated")
            self.fred_release_series_attributes[release_name]['observation_end'] = s.get("observation_end")
        return self.fred_release_series[release_name]

    def request_all_releases(self):
        response = requests.get(f"https://api.stlouisfed.org/fred/releases?api_key={self.api_key}&file_type=json")
        response_data = response.json()
        return response_data['releases']

    def request_release_series(self, release_id=None):
        if release_id is None:
            return None
        endpoint = "fred/release/series"
        release_filter = f"release_id={release_id}"
        api_key_str = f"api_key={self.api_key}"
        file_type_str = f"file_type={cfg.fred_file_type}"
        limit = f"limit={1000}"
        query_str = f"{self.fred_root_url}/{endpoint}?{release_filter}&{api_key_str}&{file_type_str}&{limit}"
        series_response = requests.get(query_str)
        return series_response.json()["seriess"]

    def request_release_table(self, release_id=None, include_observation_values='false'):
        if release_id is None:
            return None
        table_parser = FREDReleaseTableAPI(self.api_key)
        return table_parser.get_release_tables(release_id=release_id,
                                               element_id=0,
                                               include_observation_values=include_observation_values,
                                               file_type=cfg.fred_file_type)


class FREDReleaseTableAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.stlouisfed.org/fred/release/tables"

    def get_release_tables(self, release_id, element_id=0, include_observation_values='false', file_type="json"):
        """
        Get release tables data from the FRED API.

        Args:
            release_id (int): ID of the release.
            element_id (int, optional): ID of the element to retrieve data for. Defaults to 0.
            include_observation_values (bool, optional): Include observation values. Defaults to False.
            file_type (str, optional): File type of the response. Defaults to "json".

        Returns:
            dict: Parsed JSON data.
        """
        params = {
            "api_key": self.api_key,
            "release_id": release_id,
            "element_id": element_id,
            "include_observation_values": include_observation_values,
            "file_type": file_type
        }

        response = requests.get(self.base_url, params=params)
        response.raise_for_status()

        return response.json()


class FREDUpdatesAPI:
    def __init__(self):
        self.api_key = cfg.fred_api_key
        self.base_url = "https://api.stlouisfed.org/fred/series/updates"

    def get_latest_updates(self, realtime_start=None, realtime_end=None, filter_value='all', limit=1000):
        """
        Get the latest updates for series from the FRED API.

        Args:
            limit (int): Maximum number of results to return.
            sort_order (str): Order of sorting results ('asc' or 'desc').

        Returns:
            dict: The JSON response from the API.
        """
        params = {
            "api_key": self.api_key,
            "file_type": cfg.fred_file_type,
            "limit": limit,
            'realtime_start': realtime_start,
            'realtime_end': realtime_end,
            'filter_value': filter_value
        }
        params = {k: v for k, v in params.items() if v is not None}
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        return response.json()


#----------------------------------------#
# FRED Data Viewer
#----------------------------------------#

series_api = FREDSeriesAPI()
release_api = FREDReleasesAPI()
updates_api = FREDUpdatesAPI()


st.write(updates_api.get_latest_updates(realtime_start='2023-10-01', filter_value='macro'))

# Select box for releases
selected_release = st.selectbox("Select a Release", [None] + release_api.release_names_all, index=0)
selected_release_series = release_api.get_release_series(selected_release)
if selected_release_series is None:
    st.write("No release selected.")
    st.stop()

selected_release_id = release_api.fred_releases_all.get(selected_release).id
#release_table = fred_releases.request_release_table(release_id=selected_release_id)
release_series_attrs = release_api.fred_release_series_attributes.get(selected_release)
release_series_all = release_api.fred_release_series.get(selected_release)


col_freq, col_sa, col_units = st.columns(3)
col_freq.selectbox("Frequency", [None]+list(release_series_attrs.get("frequency", [])), index=0)
col_sa.selectbox("Seasonal Adjustment", [None]+list(release_series_attrs.get("sa", [])), index=0)
col_units.selectbox("Units", [None]+list(release_series_attrs.get("units", [])), index=0)

all_series_dict = {v.title: v.id for k, v in release_series_all.items()}
selected_series = st.selectbox("Select a Series", [None]+list(all_series_dict.keys()), index=0)

series_info = series_api.get_series_info(series_id=all_series_dict.get(selected_series))
st.write(series_info)



