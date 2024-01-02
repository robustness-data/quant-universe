import requests
import zipfile
import pandas as pd
import io
import os


def choose_column_language(df, chinese_columns=True):
    if chinese_columns:
        return df.rename(columns=lambda n: n.split('\n')[0])
    else:
        return df.rename(columns=lambda n: n.split('\n')[-1])


def fetch_csrcindustry(chinese_name=True):
    """
    从CSI官网下载证监会行业分类数据。来自 https://www.csindex.com.cn/#/dataService/industryClassification
    :param chinese_name: 是否返回中文行业名称
    """
    csrcindustry_url = 'https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/data_update/csrcindustry.zip'
    response = requests.get(csrcindustry_url)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    excel_file_name = [f for f in zip_file.namelist() if f.endswith('.xlsx') or f.endswith('.xls')][0]
    extracted_file = zip_file.extract(excel_file_name)
    df = pd.read_excel(extracted_file)
    df = choose_column_language(df, chinese_name)
    return df


def get_csi_industry_notes(update=False):
    if update:
        url = "https://csi-web-dev.oss-cn-shanghai-finance-1-pub.aliyuncs.com/industryClassification/%E4%B8%AD%E8%AF%81%E8%A1%8C%E4%B8%9A%E5%88%86%E7%B1%BB%E6%9E%B6%E6%9E%84.xlsx"
        csi_industry_notes=pd.read_excel(url)
        csi_industry_notes.fillna(method='ffill').set_index(['一级行业','二级行业','三级行业','四级行业'])['释义'].reset_index()
        csi_industry_notes.to_csv('csi_industry_notes.csv', index=False)
    csi_industry_notes=pd.read_csv('csi_industry_notes.csv')
    return csi_industry_notes.fillna(method='ffill').set_index(['一级行业','二级行业','三级行业','四级行业'])['释义']


def get_csi_industry_map():
    from pathlib import Path
    csi_industry_map = pd.read_excel(Path(__file__).parent/'csi_industry_map.xlsx')
    csi_level_1_map = csi_industry_map.set_index('证券代码')['中证一级行业分类简称'].to_dict()
    csi_level_2_map = csi_industry_map.set_index('证券代码')['中证二级行业分类简称'].to_dict()
    csi_level_3_map = csi_industry_map.set_index('证券代码')['中证三级行业分类简称'].to_dict()
    csi_level_4_map = csi_industry_map.set_index('证券代码')['中证四级行业分类简称'].to_dict()
    csi_tic_name_map = csi_industry_map.set_index('证券代码')['证券代码简称'].to_dict()
    return {
        'level_1': csi_level_1_map,
        'level_2': csi_level_2_map,
        'level_3': csi_level_3_map,
        'level_4': csi_level_4_map,
        'tic_name_map': csi_tic_name_map
    }