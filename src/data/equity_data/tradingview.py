import os
import pandas as pd
import numpy as np

import src.config as cfg
from src.data.database.db_manager import TradingViewDB
from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.utils.general_utils import assign_market_cap_group, check_group_by_input
import plotly.express as px


import logging
logger = logging.getLogger(__name__)


tv_univ_label_to_keys = {
    'US All Universe': 'us',
    'NASDAQ Composite': 'nasdaq-composite',
    'NASDAQ 100': 'ndx',
    'NASDAQ Golden Dragon China': 'nasdaq-china',
    'NASDAQ Biotech': 'nasdaq-biotech',
    'S&P 500': 'sp500',
    'Russell 1000': 'r1k',
    'Russell 2000': 'r2k',
    'Russell 3000': 'r3k',
    'China All Universe': 'china',
    'Hong Kong Universe': 'hongkong'
}
tv_univ_keys_to_labels = {v: k for k, v in tv_univ_label_to_keys.items()}


category_dict = {
    "General Meta": [
            'Ticker', 'Description', 'Sector', 'Industry', 'Country',
            'Number of Employees', 'Total Shares Outstanding', 'Shares Float',
            "Price", 'Market Capitalization', 'Money Flow (14)',
            "Recent Earnings Date", "Upcoming Earnings Date", "Technical Rating"
        ],
    "Performance": [
        'Change', 'Change %', "Change from Open %", 'Change 1W, %', 'Change 1M, %',
        'Weekly Performance', "Monthly Performance", '3-Month Performance', '6-Month Performance',
        'Yearly Performance', '5Y Performance', 'YTD Performance',
        '1-Month High', '3-Month High', '6-Month High', '52 Week High', 'All Time High',
        '1-Month Low', '3-Month Low', '6-Month Low', '52 Week Low', 'All Time Low',
        "1-Year Beta"
    ],
    "Trend": [
        'Exponential Moving Average (5)',
        'Exponential Moving Average (10)',
        'Exponential Moving Average (20)',
        'Exponential Moving Average (30)',
        'Exponential Moving Average (50)',
        'Exponential Moving Average (100)',
        'Simple Moving Average (5)',
        'Simple Moving Average (10)',
        'Simple Moving Average (20)',
        'Simple Moving Average (30)',
        'Simple Moving Average (50)',
        'Simple Moving Average (100)',
        'Simple Moving Average (200)',
    ],
    "Risk": [
        'Volatility', 'Volatility Week', 'Volatility Month'
    ],
    "Volume": [
        'Volume', 'Volume*Price', 'Volume Weighted Average Price',  'Volume Weighted Moving Average (20)',
        'Average Volume (10 day)', 'Average Volume (30 day)', 'Average Volume (60 day)', 'Average Volume (90 day)',
    ],
    "Balance Sheet": [
        'Total Liabilities (FY)', 'Cash & Equivalents (FY)', 'Cash and short term investments (FY)',
        'Total Assets (MRQ)', 'Total Current Assets (MRQ)', 'Cash & Equivalents (MRQ)', 'Cash and short term investments (MRQ)',
        'Total Liabilities (MRQ)', 'Total Debt (MRQ)', 'Net Debt (MRQ)',
        'Enterprise Value (MRQ)',
        'Debt to Equity Ratio (MRQ)','Quick Ratio (MRQ)', 'Current Ratio (MRQ)',

        'Total Debt (Annual YoY Growth)',
        'Total Debt (Quarterly YoY Growth)',
        'Total Debt (Quarterly QoQ Growth)',
        'Total Assets (Annual YoY Growth)',
        'Total Assets (Quarterly YoY Growth)',
        'Total Assets (Quarterly QoQ Growth)'
    ],
    "Profitability": [
        'Gross Margin (FY)', 'Net Margin (FY)', 'Free Cash Flow Margin (FY)', 'Operating Margin (FY)',
        'Gross Margin (TTM)', 'Net Margin (TTM)', 'Free Cash Flow Margin (TTM)', 'Operating Margin (TTM)',
        'Pretax Margin (TTM)',
        'Return on Assets (TTM)', 'Return on Equity (TTM)', 'Return on Invested Capital (TTM)',
    ],
    "Income Statement": [
        'Total Revenue (FY)', 'Gross Profit (FY)', 'Net Income (FY)', 'Dividends Paid (FY)',
        'EBITDA (TTM)', 'Basic EPS (TTM)', 'EPS Diluted (TTM)',
        'Revenue per Employee (FY)', 'Dividends per Share (FY)', 'EPS Diluted (FY)', 'Basic EPS (FY)',
        'EPS Forecast (MRQ)', 'EPS Diluted (MRQ)', "Dividends per Share (MRQ)", 'Gross Profit (MRQ)',

        'Revenue (Annual YoY Growth)', 'EPS Diluted (Annual YoY Growth)', 'EBITDA (Annual YoY Growth)',
        'Free Cash Flow (Annual YoY Growth)', 'Gross Profit (Annual YoY Growth)', 'Net Income (Annual YoY Growth)',
        "Dividends per share (Annual YoY Growth)",

        'Revenue (Quarterly QoQ Growth)', 'EPS Diluted (Quarterly QoQ Growth)', 'EBITDA (Quarterly QoQ Growth)',
        'Free Cash Flow (Quarterly QoQ Growth)', 'Gross Profit (Quarterly QoQ Growth)', 'Net Income (Quarterly QoQ Growth)',

        'Revenue (Quarterly YoY Growth)', 'EPS Diluted (Quarterly YoY Growth)', 'EBITDA (Quarterly YoY Growth)',
        'Free Cash Flow (Quarterly YoY Growth)', 'Gross Profit (Quarterly YoY Growth)', 'Net Income (Quarterly YoY Growth)',

        'Revenue (TTM YoY Growth)', 'EPS Diluted (TTM YoY Growth)', 'EBITDA (TTM YoY Growth)',
        'Free Cash Flow (TTM YoY Growth)', 'Gross Profit (TTM YoY Growth)', 'Net Income (TTM YoY Growth)',
    ],
    'Investment':[
        'Research & development Ratio (TTM)', 'Research & development Ratio (FY)',
    ],
    "Valuation": [
        'Price to Book (FY)', 'Price to Book (MRQ)', 'Price to Sales (FY)', "Dividend Yield Forward",
        'Price to Earnings Ratio (TTM)', 'Enterprise Value/EBITDA (TTM)',
        'Price to Free Cash Flow (TTM)', 'Price to Revenue Ratio (TTM)',
    ],

}


class TradingView:

    def __init__(self):
        self.database = TradingViewDB()
        self.data_df = pd.read_parquet(cfg.TV_CACHE_DIR/'tradingview_data.parquet')
        self.univ_label_to_keys = tv_univ_label_to_keys
        self.univ_keys_to_labels = tv_univ_keys_to_labels
        self.dates = self.data_df.Date.unique().tolist()
        self.varnames = self.data_df.columns.tolist()
        self.category_dict = category_dict
        self.sector_industry_map = self.data_df.groupby(['Sector'])\
            .apply(lambda x: x.Industry.unique().tolist()).to_dict()

        # create a dictionary of unique values for universe, sector, and industry by date
        temp_dict = {k: None for k in ['Universe','Sector','Industry']}
        for k, v in temp_dict.items():
            if v is None:
                v = self.data_df.groupby(['Date'])[k].unique().to_dict()
                setattr(self, k.lower()+'_by_date', v)

    def load_available_dates(self):
        pass

    def load_data_dt(self, dt):
        """
        Load the data for a specific date.
        :param dt: the date of analysis
        :return:
        """
        data_dt = df_filter(df=self.data_df, filter_dict={'Date': dt})
        available_univ = data_dt.Universe.unique().tolist()
        available_univ_labels = [self.univ_keys_to_labels.get(x, x) for x in available_univ]
        return {
            'data_dt': data_dt,
            'available_univ_labels': available_univ_labels
        }

    @staticmethod
    def define_asset_tags(data, **kwargs):
        data['IsETF'] = data['Description'].str.contains('ETF')
        data['MarketCapGroup'] = data['Market Capitalization'].apply(lambda x: assign_market_cap_group(x / 1e6))
        return data

    @staticmethod
    @check_group_by_input
    def calc_average_by_group(data: pd.DataFrame, groupby: list, vars: list, weight: str, reset_index: bool = True):
        """
        Calculate the grouped statistics of the columns across the groupby columns
        parameters:
            data: pd.DataFrame. A long-format dataframe with the columns to be grouped and calculated
            groupby: list: the columns to be grouped
            vars: list: the columns to be calculated
            reset_index: bool
        """
        if weight is None:
            grouped_mean = data.groupby(groupby)[vars].mean()
        else:
            dflist = []
            for var in vars:
                grouped_mean_var = data.groupby(groupby).apply(
                    lambda df: np.average(df[var], weights=df[weight], axis=0)).rename(var)
                dflist.append(grouped_mean_var)
            grouped_mean = pd.concat(dflist, axis=1)
        if reset_index:
            grouped_mean = grouped_mean.reset_index()
        return grouped_mean

    @staticmethod
    def plot_performance_heatmap(data, vars_of_interest: list, groupby: list, drilldown: list,
                                 xaxis: str, yaxis: str,
                                 xaxis_labels=None, yaxis_labels=None,
                                 weight='Market Capitalization', wins: list = None, rounding=None,
                                 dropna=True, fillna=None, dtype=None, height=600, width=1200):

        perf_by_group = TradingView.calc_average_by_group(data=data, groupby=groupby,
                                                  vars=vars_of_interest, weight=weight, reset_index=False)

        for c in vars_of_interest:
            perf_grid = perf_by_group.copy(deep=True)
            for d in drilldown:
                perf_grid = perf_grid.xs(d)
            if fillna:
                perf_grid = perf_grid.fillna(fillna)
            if dtype:
                perf_grid = perf_grid.astype(dtype)
            if wins:
                try:
                    perf_grid = perf_grid.clip(wins[0], wins[1])
                except:
                    logger.error(f"Failed to clip {c} to {wins}")
            if rounding:
                perf_grid = perf_grid.apply(lambda x: round(x, rounding))

            # formatting performance grid
            perf_grid = perf_grid.unstack(xaxis)[c]
            if xaxis_labels:
                perf_grid = perf_grid.reindex(xaxis_labels, axis=1)
            if yaxis_labels:
                perf_grid = perf_grid.reindex(yaxis_labels, axis=0)
            if dropna:
                perf_grid = perf_grid.dropna(axis=0, how='all')

            fig = px.imshow(perf_grid,
                            labels=dict(y=yaxis, x=xaxis, color=c),
                            x=perf_grid.columns, y=perf_grid.index,
                            color_continuous_scale='RdBu_r',
                            color_continuous_midpoint=0,
                            width=width, height=height,
                            text_auto=True)
            fig.update_layout(title_text=c)
            fig.show()

    @staticmethod
    def plot_asset_count_heatmap(data: pd.DataFrame, groupby: list, drilldown: list,
                                 xaxis: str, yaxis:str,
                                 xaxis_labels=None, yaxis_labels=None,
                                 fillna=None, dtype=None):
        asset_count_by_group = data.groupby(group_keys=groupby).count()['Ticker']
        data_to_plot = asset_count_by_group.copy()
        for d in drilldown:
            data_to_plot = data_to_plot.xs(d)
        data_to_plot = data_to_plot.unstack(xaxis)
        if xaxis_labels:
            data_to_plot = data_to_plot.reindex(xaxis_labels, axis=1)
        if yaxis_labels:
            data_to_plot = data_to_plot.reindex(yaxis_labels, axis=0)
        if fillna:
            data_to_plot = data_to_plot.fillna(fillna)
        if dtype:
            data_to_plot = data_to_plot.astype(dtype)

        fig = px.imshow(
            data_to_plot,
            zmin=0, zmax=500, color_continuous_scale='Blues', text_auto=True,
            title='Asset Count by Groups',
            labels=dict(x=xaxis, y=yaxis, color='Asset Count'),
            width=1100, height=500
        )
        return fig


class BigA(TradingView):

    performance_translator = {
        'Change %': '涨跌幅',
        'Change': '变化',
        '3-Month Performance': '三个月表现',
        '5Y Performance': '五年表现',
        '6-Month Performance': '六个月表现',
        'Change 1M, %': '过去一个月涨跌幅',
        'Change 1W, %': '过去一周涨跌幅',
        'YTD Performance': '年初至今表现',
        'Weekly Performance': '周表现',
        'Yearly Performance': '年度表现',
        'Monthly Performance': '月度表现',
        'Change from Open %': '开盘以来变化百分比'
    }

    def __init__(self):
        super().__init__()
        self.get_csi_meta_map()  # 从中证指数官网获取中证行业分类数据
        self.asset_tag_supplement_data = {
            'csi_industry_map': self.csi_industry_map,
            'csi_tic_name_map': self.csi_tic_name_map
        }

    def get_csi_meta_map(self):
        import src.meta.ashare_params as ashare_meta
        self.csrc_sector_map = ashare_meta.fetch_csrcindustry(True)
        self.csi_industry_map = ashare_meta.get_csi_industry_map()
        self.csi_tic_name_map = self.csi_industry_map.get('tic_name_map')
        self.csi_ashare_tickers = [k for k in self.csi_tic_name_map.keys() if '.HK' not in k]
        self.csi_hek_tickers = [k for k in self.csi_tic_name_map.keys() if '.HK' in k]

    def load_data_cache_from_csv(self, dt):
        from src.config import TV_CACHE_DIR
        data_dir = TV_CACHE_DIR/'raw'/'china'
        dflist = []
        for f in os.listdir(data_dir):
            if ('.csv' in f) and (dt in f):
                df = pd.read_csv(os.path.join(data_dir,f))
                df['Date'] = f.split('_')[1]
                df['Universe'] = f.split('_')[0].upper()
                try:
                    df['Time'] = f.split('_')[2].replace('.csv','')
                except:
                    df['Time'] = 'close'
                dflist.append(df)
        if len(dflist) == 0:
            logging.info(f"No data found for {dt}")
            return None
        data = pd.concat(dflist)
        try:
            logger.info("Populating asset tags...")
            return BigA.define_asset_tags(data, **self.asset_tag_supplement_data)
        except Exception as e:
            logger.error(f"Failed to populate asset tags: {e}")
            return

    @staticmethod
    def define_asset_tags(data: pd.DataFrame, **kwargs):
        def convert_to_ticker(x: int):
            return str(x).zfill(6)
        data['tic'] = data['Ticker'].apply(convert_to_ticker)
        data['IsETF'] = data['Description'].str.contains('ETF')
        data['MarketCapGroup'] = data['Market Capitalization'].apply(lambda x: BigA.assign_market_cap_group(x/1e8))
        csi_industry_map = kwargs.get('csi_industry_map')
        csi_tic_name_map = kwargs.get('csi_tic_name_map')
        data = data \
            .assign(sector_csi_level_1 = lambda x: x['tic'].map(csi_industry_map.get('level_1')))\
            .assign(sector_csi_level_2 = lambda x: x['tic'].map(csi_industry_map.get('level_2')))\
            .assign(sector_csi_level_3 = lambda x: x['tic'].map(csi_industry_map.get('level_3')))\
            .assign(sector_csi_level_4 = lambda x: x['tic'].map(csi_industry_map.get('level_4')))\
            .assign(name_cn = lambda x: x['tic'].map(csi_tic_name_map))
        
        stocks = data[(~data['IsETF']) & (~data['name_cn'].isna())]
        foreign_etf = data[(~data['IsETF']) & (data['name_cn'].isna())]
        etfs = data[data['IsETF']]
        logger.info(f"Successfully loaded {len(stocks)} stocks and {len(etfs)} ETFs")
        return stocks, etfs, foreign_etf
    
    @staticmethod
    def assign_market_cap_group(x):
        import numpy as np
        """根据市值设定市值分组，单位为亿元"""
        if np.isnan(x) or (x is None) or (not isinstance(x,float)):
            return 'N/A'
        if x >= 5000:
            return "超大盘股"
        elif 500 <= x < 5000:
            return "大盘股"
        elif 100 <= x < 500:
            return "中盘股"
        elif 10 <= x < 100:
            return "小盘股"
        elif 1 <= x < 10:
            return "微盘股"
        elif 0 < x < 1:
            return "超微盘股"
        else:
            return "N/A"

    @check_group_by_input
    def calc_average_across_group(data: pd.DataFrame, groupby: list, vars: list, weight: str, reset_index: bool = True):
        """
        Calculate the grouped statistics of the columns across the groupby columns
        parameters:
            data: pd.DataFrame. A long-format dataframe with the columns to be grouped and calculated
            groupby: list: the columns to be grouped
            vars: list: the columns to be calculated
            reset_index: bool
        """
        import pandas as pd
        import numpy as np
        import warnings
        warnings.filterwarnings("ignore")
        if weight is None:
            grouped_mean = data.groupby(groupby)[vars].mean()        
        else:
            dflist = []
            for var in vars:
                grouped_mean_var = data.groupby(groupby).apply(lambda df: np.average(df[var], weights=df[weight], axis=0)).rename(var)
                dflist.append(grouped_mean_var)
            grouped_mean = pd.concat(dflist, axis=1)
        if reset_index:
                grouped_mean = grouped_mean.reset_index()
        return grouped_mean

    @staticmethod
    def prepare_metrics(stocks):
        return stocks\
            .assign(ytd_cost=lambda x: x['Price']*x['YTD Performance'].apply(lambda y: 1+y/100))\
            .assign(CostLE10=lambda x: x['ytd_cost']<=10) \
            .assign(logPrice=lambda x: np.log(x['Price'])) \
            .assign(logYTDCost=lambda x: np.log(x['ytd_cost']))\
            .assign(logVol=lambda x: x['YTD Performance']/x['Volatility'])\
            .assign(SharpRatio=lambda x: x['YTD Performance']/x['Volatility'])

    @staticmethod
    def top_sharp_ratios(stocks):
        metrics = BigA.prepare_metrics(stocks)
        top_names = metrics.sort_values(by='SharpRatio', ascending=False).head(100)
        print(f"大A股中，年初至今经风险调整过后表现最好的前100只股票：")
        for i, row in top_names.iterrows():
            print(
                '名称:', row.name_cn,
                "| 代码:", row.tic,
                "| 年初价格:", round(row.ytd_cost, 3),
                #"| 英文名：", row.Description,
                "| 2023年表现:", "{:.2f}%".format(row['YTD Performance']),
                "| 波动率:", round(row.Volatility, 3),
                "| 行业：", row.sector_csi_level_2
            )

    @staticmethod
    def alpha_box(stocks):
        # box plot
        fig = px.box(BigA.prepare_metrics(stocks),
                     x='sector_csi_level_2',
                     y='YTD Performance',
                     color='CostLE10', width=1200, height=500)
        fig.update_layout(
            title_text='A股各个行业的年初至今表现',
            xaxis_title_text='中证二级行业',
            yaxis_title_text='年初至今表现',
            legend_title_text='是否小于10元',
            plot_bgcolor='rgba(0,0,0,0)',
        )
        fig.add_hline(y=0, line_width=1, line_dash="solid", line_color="gray")
        fig.show()

    @staticmethod
    def alpha_scatter(stocks):
        plot_data = BigA.prepare_metrics(stocks)

        label_map = {
            'YTD Performance': '年初至今表现',
            'Volatility': '波动率/风险',
            'SharpRatio': '风险调整后收益'
        }
        for k, label in label_map.items():
            # scatter plot
            fig = px.scatter(
                plot_data,
                x='logYTDCost', y=k,
                color='CostLE10', hover_data=['name_cn','ytd_cost'],
                width=800, height=500)
            # update layout
            fig.update_layout(
                title_text=f'{label} vs. 年初成本价',
                xaxis_title_text='log(年初成本价)',
                yaxis_title_text=label,
                legend_title_text='小于10元',
                plot_bgcolor='rgba(0,0,0,0)',
                template='plotly_dark'
            )
            fig.show()

        # scatter plot
        fig = px.scatter(
            plot_data.sort_values('SharpRatio',ascending=False),
            x='Volatility', y='YTD Performance',
            color='CostLE10', hover_data=['name_cn','ytd_cost'],
            width=800, height=500)
        # update layout
        fig.update_layout(
            title_text='Frontier',
            xaxis_title_text='波动率/风险',
            yaxis_title_text='年初至今表现',
            legend_title_text='年初成本价',
            plot_bgcolor='rgba(0,0,0,0)',
            template='plotly_dark'
        )
        fig.show()

    @staticmethod
    def alpha_histogram(stocks):
        plot_data = BigA.prepare_metrics(stocks)
        label_map = {
            'YTD Performance': '年初至今表现',
            'Volatility': '波动率/风险',
            'SharpRatio': '风险调整后收益'
        }
        for x in ['YTD Performance','Volatility','SharpRatio']:
            fig = px.histogram(
                plot_data, x=x, color='CostLE10', nbins=200,
                marginal='box', width=800, height=500)
            # update layout
            fig.update_layout(
                title_text=label_map.get(x),
                xaxis_title_text=label_map.get(x),
                yaxis_title_text='Count',
                legend_title_text='小于10元',
                plot_bgcolor='rgba(0,0,0,0)',
                template='plotly_dark'
            )
            fig.show()


if __name__ == "__main__":

    tv = TradingView()

    #print(tv.universe_list)
    #print(tv.dates)
    #print(tv.varnames)
    #print(tv.sector_names)
    #print(tv.industry_names)
    #print(tv.sector_industry_map)

    #tv.database.drop_table('universe')  # TODO: fix issues here
    #tv.database.create_universe_table()
    #tv.database.populate_universe_table(universe_df=tv.data_df)