import os, sys
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent
EQ_CACHE_DIR = ROOT_DIR / 'data' / 'equity_market'
ETF_CACHE_DIR = EQ_CACHE_DIR / '1_ishares_etf'
TV_CACHE_DIR = EQ_CACHE_DIR / '3_tradingview'
MACRO_CACHE_DIR = ROOT_DIR / 'data' / 'macro'
FED_CACHE_DIR = MACRO_CACHE_DIR / 'Fed'
TSY_CACHE_DIR = MACRO_CACHE_DIR / 'USTreasury'
DB_DIR = ROOT_DIR / 'database'
META_DIR = ROOT_DIR / 'meta_data'
