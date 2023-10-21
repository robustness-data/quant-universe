import requests
from string import Template
from collections import namedtuple
import pandas as pd
import numpy as np


base_url = 'https://www.federalreserve.gov/datadownload/Output.aspx?'
query_suffix = '&lastobs=&from=&to=&filetype=csv&label=include&layout=seriescolumn&type=package'
query_template = Template(base_url + "rel=H8&series=${series_name}" + query_suffix)
series_code_map = {
    'all_commercial': '5375da96d8d95fb3b5b6395771a324a9',
    'domestic': '787e541e2c5924f1c094b139a5782e3e',
    'foreign': '353100ce516d5cad9e129f138ef99512',
    'large_domestic': '34292880b80cb12a042b407d2f234be6',
    'small_domestic': '240633ea191014c4163f530e199fe4a3'
}
suffix_map = {
    'all_commercial': '|all|nsa',
    'domestic': '|domestic|nsa',
    'foreign': '|foreign|nsa',
    'large_domestic': '|large|nsa',
    'small_domestic': '|small|nsa'
}


class FederalReserveH8:

    def __init__(self):
        self.url_map = {k: query_template.substitute(series_name=v) for k, v in series_code_map.items()}
        self.H8 = namedtuple('H8', ['level', 'parent', 'name'])
        self.create_balance_sheet_tree()
        self.raw_data_cache = {}
        self.data_cache = {}
        self.code2label_map = {}
        self.label2code_map = {}

    def create_balance_sheet_tree(self):
        self.level_1_assets = (
            'bank_credit',
            'cash_assets',
            'total_fed_funds_sold_and_securities_purchased_under_agreements_to_resell',
            'loans_to_commercial_banks',
            'other_assets',
            'total_assets'
        )

        self.level_1_liabilities = (
            'deposits',
            'borrowings',
            'net_due_to_related_foreign_offices',
            'other_liabilities',
            'total_liabilities'
        )

        self.level_2_bank_credit = ('securities_in_bank_credit', 'loans_and_leases_in_bank_credit')
        self.level_3_securities = ('treasury_and_agency_securities', 'other_securities')
        self.level_4_treasury_securities = ('mortgage-backed_securities_(mbs)', 'non-mbs')
        self.level_4_other_securities = ('mortgage-backed_securities', 'non-mbs')

        self.level_3_loans = ('commercial_and_industrial_loans', 'real_estate_loans',
                              'consumer_loans', 'other_loans_and_leases:all_other_loans_and_leases')
        self.level_4_real_estate_loans = ('residential_real_estate_loans', 'commercial_real_estate_loans')
        self.level_5_rre_loans = ('revolving_home_equity_loans', 'closed-end_residential_loans')
        self.level_5_cre_loans = ('construction_and_land_development_loans', 'secured_by_farmland',
                                  'secured_by_multifamily_properties', 'secured_by_nonfarm_nonresidential_properties')
        self.level_4_consumer_loans = ('credit_cards_and_other_revolving_plans', 'other_consumer_loans')
        self.level_5_other_consumer_loans = ('automobile_loans', 'all_other_consumer_loans')
        self.level_4_other_loans = ('loans_to_nondepository_financial_institutions',
                                    'other_loans_not_elsewhere_classified')

        h8tree = []
        for item in self.level_1_assets:
            h8tree.append(self.H8(1, 'assets', item))
        for item in self.level_1_liabilities:
            h8tree.append(self.H8(1, 'liabilities', item))
        for item in self.level_2_bank_credit:
            h8tree.append(self.H8(2, 'bank_credit', item))
        for item in self.level_3_securities:
            h8tree.append(self.H8(3, 'securities_in_bank_credit', item))
        for item in self.level_3_loans:
            h8tree.append(self.H8(3, 'loans_and_leases_in_bank_credit', item))
        for item in self.level_4_treasury_securities:
            h8tree.append(self.H8(4, 'treasury_and_agency_securities', item))
        for item in self.level_4_other_securities:
            h8tree.append(self.H8(4, 'other_securities', item))
        for item in self.level_4_real_estate_loans:
            h8tree.append(self.H8(4, 'real_estate_loans', item))
        for item in self.level_4_consumer_loans:
            h8tree.append(self.H8(4, 'consumer_loans', item))
        for item in self.level_4_other_loans:
            h8tree.append(self.H8(4, 'other_loans_and_leases:all_other_loans_and_leases', item))
        for item in self.level_5_rre_loans:
            h8tree.append(self.H8(5, 'residential_real_estate_loans', item))
        for item in self.level_5_cre_loans:
            h8tree.append(self.H8(5, 'commercial_real_estate_loans', item))
        for item in self.level_5_other_consumer_loans:
            h8tree.append(self.H8(5, 'other_consumer_loans', item))

        self.h8tree = h8tree

    def build_h8_cache(self, series_name: str):
        series_url = self.url_map.get(series_name)
        raw_data = self.get_data(series_url)
        code2label, label2code, data = self.clean_data(raw_data)

        self.code2label_map[series_name] = code2label
        self.label2code_map[series_name] = label2code
        self.raw_data_cache[series_name] = raw_data
        self.data_cache[series_name] = data

    def get_level3_holdings(self, series_name):
        data = self.data_cache.get(series_name)
        label2code = self.label2code_map.get(series_name)
        suffix = suffix_map.get(series_name)
        dflist = []
        for node in self.h8tree:
            if node.level == 3:
                dflist.append(data[label2code.get(node.name + suffix)].rename(node.name))
        return pd.concat(dflist, axis=1) / 1e3

    def get_cre_holdings(self, series_name):
        data = self.data_cache.get(series_name)
        label2code = self.label2code_map.get(series_name)
        suffix = suffix_map.get(series_name)
        dflist = []
        for node in self.h8tree:
            if (node.parent == 'commercial_real_estate_loans') and (node.level == 5):
                item_code = label2code.get('real_estate_loans:' + node.parent + ':' + node.name + suffix)
                dflist.append(data[item_code].rename(node.name))
        return pd.concat(dflist, axis=1) / 1e3

    def get_rre_holdings(self, series_name):
        data = self.data_cache.get(series_name)
        label2code = self.label2code_map.get(series_name)
        suffix = suffix_map.get(series_name)
        dflist = []
        for node in self.h8tree:
            if (node.parent == 'residential_real_estate_loans') and (node.level == 5):
                item_code = label2code.get('real_estate_loans:' + node.parent + ':' + node.name + suffix)
                dflist.append(data[item_code].rename(node.name))
        return pd.concat(dflist, axis=1) / 1e3

    @staticmethod
    def clean_names(x):
        return (
            x.replace(', ', '|').replace(': ', ':').replace('"', '')
            .replace(' ', '_')
            # .replace('-','_')
            .replace('not_seasonally_adjusted', 'nsa')
            .replace('seasonally_adjusted', 'sa')
            .replace('all_commercial_banks', 'all')
            .replace('large_domestically_chartered_commercial_banks', 'large')
            .replace('small_domestically_chartered_commercial_banks', 'small')
            .replace('foreign-related_institutions', 'foreign')
        )

    @staticmethod
    def get_data(url):
        response = requests.get(url)
        if response.status_code == 200:
            response_data = response.content.decode('utf-8')
        else:
            raise Exception("failed to get data.")

        input_list = [FederalReserveH8.clean_names(row).split(',') for row in response_data.splitlines()]
        input_list = [[x.lower() for x in row] for row in input_list]
        data_df = pd.DataFrame(input_list)

        return data_df

    @staticmethod
    def clean_data(raw_data):
        code2label = raw_data.iloc[[0, 5], 1:].T.set_index(5)[0].to_dict()
        label2code = {v: k for k, v in code2label.items()}

        data = pd.DataFrame(data=raw_data.iloc[6:, 1:].values,
                            columns=raw_data.iloc[5, 1:].values,
                            index=raw_data.iloc[6:, 0].values)
        data.index.name = 'date'
        data.columns.name = 'item'
        data = data.rename(pd.to_datetime)

        for item in data.columns:
            data[item] = data[item].replace('', np.nan).astype(float)

        return code2label, label2code, data

    @staticmethod
    def wide_to_long(series_df, groupname):
        return (
            series_df.stack().rename('value')
            .reset_index().rename(columns={'level_1': 'item'})
            .assign(group=lambda x: groupname)
        )


if __name__ == '__main__':
    h8 = FederalReserveH8()
    h8.build_h8_cache('foreign')
    level3_df = h8.get_level3_holdings('foreign')
    cre_df = h8.get_cre_holdings('foreign')
    print(cre_df.to_string())