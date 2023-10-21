import os, sys
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent
EQ_CACHE_DIR = ROOT_DIR / 'data' / 'equity_market'
TV_CACHE_DIR = EQ_CACHE_DIR / '3_tradingview'
DB_DIR = ROOT_DIR / 'database'
META_DIR = ROOT_DIR / 'meta_data'
