import requests
from bs4 import BeautifulSoup, NavigableString
import pandas as pd
import sqlite3

import logging
logger = logging.getLogger(__name__)
import os
import re
from datetime import datetime
import time
import random
from pathlib import Path
from tqdm import tqdm
from io import StringIO


ROOT_DIR=Path(__file__).parent.parent.parent.parent
ISHARES_URL_DIR=ROOT_DIR/'src'/'meta'/'ishares_etf'
DB_DIR=ROOT_DIR/'database'
if not os.path.exists(DB_DIR):
    print('Creating database directory')
    os.makedirs(DB_DIR)


def get_all_etf_urls():
    # get all the ETF urls info
    etf_meta = []
    for f in os.listdir(ISHARES_URL_DIR):
        etf_meta.append(pd.read_csv(ISHARES_URL_DIR/f))
    etf_meta = pd.concat(etf_meta)
    etf_meta.drop_duplicates(inplace=True)

    all_etf_urls = dict()
    base_url = 'https://www.ishares.com/us/products'
    for i, row in tqdm(etf_meta.iterrows(), total=etf_meta.shape[0], desc=f'Scraping ETF stats'):
        etf_id = row['etf-id']
        etf_name = row['etf_name']
        etf_ticker = row['ticker']
        etf_url = f"{base_url}/{etf_id}/{etf_name}"
        all_etf_urls[etf_ticker] = etf_url
    return all_etf_urls


def get_etf_response(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    return soup


def get_etf_stats():

    all_etf_urls = get_all_etf_urls()
    etf_stats = dict()
    for etf_ticker, url in tqdm(all_etf_urls.items()):
        etf_stats[etf_ticker] = None  # default to be updated

        # get ETF stats from the url with retry logic
        number_of_retries = 5
        for i in range(number_of_retries):
            try:
                soup = get_etf_response(url)
                header_dict = parse_header(soup)
                tables_dict = parse_all_tables(soup)
                divs_dict = parse_all_divs(soup)
                etf_stats[etf_ticker] = {**header_dict, **tables_dict, **divs_dict}
                break
            except Exception as e:
                print(f"Error when scraping {etf_ticker.upper()}: {e}")
                continue

        # if failed to scrape, skip
        if etf_stats[etf_ticker] is None:
            continue

        # collect results and write into DB
        for k, df in etf_stats[etf_ticker].items():
            if df is None:
                continue
            try:
                today = datetime.today().date().isoformat()
                write_new_data_to_db(df.assign(ticker=etf_ticker), k, str(DB_DIR / f'etf_{today}.db'))
            except Exception as e:
                print(f"Error when writing {etf_ticker.upper()} {k} to DB: {e}")
            #conn = sqlite3.connect(str(DB_DIR / 'etf.db'))
            #print(pd.read_sql(f"SELECT * FROM {k}", conn).to_string())
        #break  # for testing

    return etf_stats


def parse_header(soup):
    header_dict = dict()
    header_dict['etf_name'] = soup.find('h1').contents[0].strip()

    nav_spans = soup \
        .find('div', id='fundheaderTabs') \
        .find('div', class_='nav-price') \
        .find('ul').find('li', class_='navAmount')
    if nav_spans:
        nav_value = nav_spans.find('span', class_='header-nav-data').text
        nav_date = nav_spans.find('span', class_='header-nav-label navAmount').text
        nav_value = float(nav_value.strip().replace('$','').replace(',',''))
        nav_date = nav_date.replace('NAV as of ','').strip()
        header_dict['nav_value'] = nav_value
        header_dict['nav_date'] = nav_date

    nav_change_spans = soup\
        .find('div', id='fundheaderTabs')\
        .find('div', class_='nav-price')\
        .find('ul').find('li', class_='navAmountChange')
    if nav_change_spans:
        nav_change_value = nav_change_spans.find('span', class_='header-nav-data').text
        nav_change_date = nav_change_spans.find('span', class_='header-nav-label navAmountChange').text
        nav_change_date = nav_change_date.replace('1 Day NAV Change as of ','').strip()
        nav_change_value, nav_change_value_pct = nav_change_value.replace('\n',' ').strip().split(' ')
        nav_change_value = float(nav_change_value.strip())
        nav_change_value_pct = float(nav_change_value_pct.strip()[1:-1].replace('%',''))/100
        header_dict['nav_change_value'] = nav_change_value
        header_dict['nav_change_value_pct'] = nav_change_value_pct
        header_dict['nav_change_date'] = nav_change_date

    ytd_perf_spans = soup \
        .find('div', id='fundheaderTabs') \
        .find('div', class_='nav-price') \
        .find('ul').find('li', class_='yearToDate')
    if ytd_perf_spans:
        ytd_perf_value = ytd_perf_spans.find('span', class_='header-nav-data').text
        ytd_perf_date = ytd_perf_spans.find('span', class_='header-nav-label yearToDate').text
        ytd_perf_date = ytd_perf_date.replace('NAV Total Return as of ','').strip()
        ytd_perf_value = float(ytd_perf_value.strip().replace('%',''))/100
        header_dict['ytd_perf_value'] = ytd_perf_value
        header_dict['ytd_perf_date'] = ytd_perf_date

    header_df = {'header': pd.DataFrame({'value': header_dict}).T}
    return header_df


def parse_all_tables(soup):
    tables = soup.find_all('table')
    tables_dict = {'average_annual_performance': None,
                   'cumulative_performance': None,
                   'calendar_year_performance': None,
                   'fee_table': None}

    for table in tables:
        try:
            df = pd.read_html(str(table))[0]
            if df.empty:
                continue
            if df.shape == (5,6):
                df.rename(columns={'Unnamed: 0': 'performance_type'}, inplace=True)
                df = df.set_index('performance_type')
                df.columns.name = 'performance_horizon'
                df = df.stack().rename('average_annual_performance').reset_index()
                tables_dict['average_annual_performance'] = df
            elif df.shape == (5,10):
                df.rename(columns={'Unnamed: 0': 'performance_type'}, inplace=True)
                df = df.set_index('performance_type')
                df.columns.name = 'performance_horizon'
                df = df.stack().rename('cumulative_performance').reset_index()
                tables_dict['cumulative_performance'] = df
            elif df.shape == (3,6):
                df.rename(columns={'Unnamed: 0': 'performance_type'}, inplace=True)
                df = df.set_index('performance_type')
                df.columns.name = 'year'
                df = df.stack().rename('calendar_year_performance').reset_index()
                tables_dict['calendar_year_performance'] = df
            elif df.shape == (4, 2):
                df = df.rename(columns={0:'fee_type', 1:'fee_value_pct'})\
                    .assign(fee_value_pct=lambda x: x['fee_value_pct'].str.replace('%',''))
                tables_dict['fee_table'] = df
            else:
                continue
        except:
            pass

    return tables_dict


def parse_div(div):
    div_output = {'as_of_date': None, 'meta_name': None, 'meta_value': None}
    # extract meta_name
    caption_span = div.find('span', class_='caption')
    net_assets_text = ''.join([str(element) for element in caption_span.contents
                               if isinstance(element, NavigableString)]).strip()
    div_output['meta_name'] = net_assets_text

    # extract meta_value
    value_span = div.find('span', class_='data')
    value_text = value_span.text.replace('\n', '').replace('$', '').replace(',', '').strip()
    if '%' in value_text:
        value_text = float(value_text.replace('%', '')) / 100
    try:
        value_text = float(value_text)
    except:
        pass
    div_output['meta_value'] = value_text

    # extract as_of_date
    date_span = div.find('span', class_='as-of-date')
    date_str = date_span.text.replace('\n', '').replace('as of', '').strip()
    date_iso_str = pd.to_datetime(date_str).date().isoformat()
    div_output['as_of_date'] = date_iso_str

    return div_output


def parse_section(soup, section_id):
    section_div = soup.find_all('div', id=section_id)
    section_output = []
    divs = section_div[0].find_all('div')[1:]
    for div in divs:
        try:
            section_output.append(parse_div(div))
        except:
            pass
    section_df = pd.DataFrame(section_output)
    return section_df


def parse_all_divs(soup):
    section_id_list = ('keyFundFacts', 'esgAnalytics', 'productInvolvement', 'feeTable', 'holdings', 'distributions')
    divs_dict = {}
    for section_id in section_id_list:
        try:
            divs_dict[section_id] = parse_section(soup, section_id)
        except:
            pass
    divs_dict = {k: v for k, v in divs_dict.items() if not v.empty}
    return divs_dict


def write_new_data_to_db(df, table_name, db_path, echo=False):
    """
    Writes new data from a pandas DataFrame to a SQLite database table.

    Parameters:
    df (pandas.DataFrame): The DataFrame to write to the database.
    table_name (str): The name of the table in the database.
    db_path (str): The path to the SQLite database file.
    """
    with sqlite3.connect(db_path) as conn:
        # Try to load existing data from the table
        try:
            existing_data = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        except pd.io.sql.DatabaseError:
            # If the table does not exist, all data is new
            existing_data = pd.DataFrame()

        # Compare and find new data
        if not existing_data.empty:
            # Ensure the comparison is on the same columns and in the same order
            common_columns = df.columns.intersection(existing_data.columns)
            new_data = df[~df[common_columns].apply(tuple,1).isin(existing_data[common_columns].apply(tuple,1))]
        else:
            new_data = df

        # Insert new data
        if not new_data.empty:
            new_data = new_data.assign(last_updated=datetime.now())
            new_data.to_sql(table_name, conn, if_exists='append', index=False)
            if echo:
                print(f"Inserted {len(new_data)} new rows into {table_name}.")
        else:
            if echo:
                print("No new data to insert.")

    conn.close()


if __name__ == '__main__':

    get_etf_stats()

    today = datetime.today().date().isoformat()
    conn = sqlite3.connect(str(DB_DIR / f'etf_{today}.db'))
    table_list = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn).name.tolist()
    for t in table_list:
        print(f"\n\n=============== {t} ====================")
        print(pd.read_sql("SELECT * FROM {}".format(t), conn).to_string())

    # list all the tables
    #print(pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn).to_string())