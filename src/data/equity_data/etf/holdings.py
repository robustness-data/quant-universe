import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import io, os, sys
import pandas as pd
from pathlib import Path

ROOT_DIR=Path(__file__).parent.parent.parent.parent.parent
DB_DIR=ROOT_DIR/'database'
if not os.path.exists(DB_DIR):
    print('Creating database directory')
    os.makedirs(DB_DIR)


#========================================== Scraper =========================================#
def get_etf_holdings_text(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.content
        else:
            print(f"Failed to download file: status code {response.status_code}")
            return 

    except Exception as e:
        print(f"An error occurred: {e}")
        return


def scrape_webpage(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to download file: status code {response.status_code}")
        return
    soup = BeautifulSoup(response.content, 'html.parser')
    return soup

#================================== ETF URLs ======================================#
# direxion_etfs
direxion_etfs = {
    "Daily Bull & Bear 3X ETFs": ["MIDU", "TNA", "TZA", "SPXL", "SPXS", "YINN", "YANG", "EURL", "EDC", "EDZ", "MEXX", "KORU", "TYD", "TYO", "TMF", "TMV", "WEBL", "WEBS", "HIBL", "HIBS", "LABU", "LABD", "FAS", "FAZ", "CURE", "NAIL", "DRN", "DRV", "DPST", "RETL", "SOXL", "SOXS"],
    "Daily Bull & Bear 2X ETFs": ["BRZU", "CHAU", "CWEB", "ERX", "ERY", "GUSH", "DRIP", "INDL", "JNUG", "JDST", "NUGT", "DUST", "SPUU", "UBOT", "CLDL", "OOTO", "KLNE", "FNGG", "EVAV"],
    "Daily Bear 1X ETFs": ["SPDN"],
    "Single Stock ETFs": ["AAPU", "AAPD", "TSLL", "TSLS", "AMZU", "AMZD", "GGLL", "GGLS", "MSFU", "MSFD", "NVDU", "NVDD"],
    "Actively Managed Tactical ETFs": ["HCMT"],
    "Non-Leveraged ETFs": ["COM", "HJEN", "MOON", "QQQE", "WFH"]
}
direxion_etfs_all = []
for k,v in direxion_etfs.items():
    direxion_etfs_all += v
direxion_etfs_urls = {x: f"https://www.direxion.com/holdings/{x}.csv" for x in direxion_etfs_all}


# ARK ETFs
ark_etfs_urls = {
    "ARKK": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
    "ARKW": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
    "ARKQ": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_AUTONOMOUS_TECH._&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
    "ARKG": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv",
    "ARKF": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv",
    "ARKX": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv",
    "ARKB": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_21SHARES_BITCOIN_ETF_ARKB_HOLDINGS.csv"
}

# SPDR ETFs
spdr_etfs_urls = {
    "XBI": "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-xbi.xlsx",
}

#================================== ETF Holdings Parser ======================================#
def parse_ark_holdings(text):
    df = pd.read_csv(io.StringIO(text.decode('utf-8'))).iloc[:-1,:]
    df['date'] = df['date'].apply(pd.to_datetime).apply(lambda dt: dt.date().isoformat())
    ark_renamer = {
        'date':'date',
        'fund':'etf_ticker',
        'company':'security_name',
        'ticker':'ticker',
        'cusip':'cusip',
        'shares':'shares',
        'market value ($)':'market_value',
        'weight (%)':'weight'
    }
    df.rename(columns=ark_renamer,inplace=True)
    df['weight'] = df['weight'].apply(lambda x: float(x[:-1])/100)
    df['shares'] = df['shares'].apply(lambda x: int(x.replace(',','')))
    df['market_value'] = df['market_value'].apply(lambda x: float(x.replace(',','').replace('$','')))
    return df


def parse_direxion_holdings(text):
    df = pd.read_csv(io.StringIO(text.decode('utf-8')),skiprows=5)
    df['date'] = df['TradeDate'].apply(lambda dt: dt.split(' ')[0]).apply(pd.to_datetime).apply(lambda dt: dt.date().isoformat())
    direxion_renamer = {
        'AccountTicker':'etf_ticker',
        'StockTicker':'ticker',
        'SecurityDescription':'security_name',
        'Shares':'shares',
        'Price': 'price',
        'MarketValue':'market_value',
        'Cusip': 'cusip',
        'HoldingsPercent': 'weight'
    }
    df.rename(columns=direxion_renamer,inplace=True)
    df['weight'] = df['weight'].apply(lambda x: x/100)
    return df.drop(columns=['TradeDate'])

#================================== Main Workflow ======================================#
def main():
    print("Root Dir:",ROOT_DIR)
    print("DB Dir:",DB_DIR)
    import sqlite3, os, datetime
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')

    # get the tables in the database
    existing_info = None
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'",conn)['name'].values
    if 'etf_holdings' in tables:
        print("================= Check existing data =================")
        existing_info = pd.read_sql("select distinct etf_ticker date from etf_holdings",conn)
        total_number_of_etfs = len(existing_info['etf_ticker'].unique())
        last_update_date = existing_info['date'].max()
        print(f"Found existing data: {total_number_of_etfs} ETFs in total, last update date {last_update_date}")

    print("================= Download holdings text =================")
    etf_urls = {**direxion_etfs_urls, **ark_etfs_urls}
    holdings_data = {}
    for tic, url in tqdm(etf_urls.items()):
        holdings_data[tic] = get_etf_holdings_text(url)

    print("================= Parse holdings text to formatted csv =================")
    holdings_csv = {}
    for tic, text in tqdm(holdings_data.items()):
        if tic in ark_etfs_urls:
            try:
                holdings_csv[tic] = parse_ark_holdings(text)
            except:
                print(f"Failed to parse {tic}")
        elif tic in direxion_etfs_all:
            try:
                holdings_csv[tic] = parse_direxion_holdings(text)
            except:
                print(f"Failed to parse {tic}")
    
    print('================== Removing existing data ==================')
    if existing_info is not None:
        for tic, df in holdings_csv.items():
            if tic in existing_info['etf_ticker'].values:
                if df['date'].iloc[0] in existing_info[existing_info['etf_ticker']==tic]['date'].values:
                    print(f"{tic} {df['date'].iloc[0]} already existed")
                    holdings_csv.pop(tic)

    print("================= Inserting new data to database =================")
    pd.concat([df for df in holdings_csv.values()])\
        .assign(update_datetime=datetime.datetime.now().isoformat())\
        .to_sql('etf_holdings',conn,if_exists='append',index=False)

    conn.close()


if __name__ == "__main__":
    main()