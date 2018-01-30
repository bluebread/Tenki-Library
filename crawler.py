"""
A WEATHER CRAWLER
"""
from datetime import date, timedelta, datetime
import re
import time
from random import random

from selenium import webdriver
from bs4 import BeautifulSoup as BS
import numpy as np
from pandas import DataFrame, concat, read_csv

print("CRAWLER: IMPORT COMPLETED!")

BROWSER = webdriver.Chrome()
# BROWSER = webdriver.PhantomJS()

ENTRY_DICT = {
    "weather_entry": "天気",
    "total_cloud_cover_entry": "雲量",
    "ceiling_entry": '雲底高度',
    "temp_entry": '気温(℃)',
    "humidity_entry": '湿度(%)',
    "dp_temp_entry": '露点温度(℃)',
    "precip_entry": '3時間降水量',
    "pressure_change_entry": '現地気圧(hPa)',
    "pressure_entry": '現地気圧(hPa)(変化量)',
    "sea_level_pressure_entry": '海面気圧(hPa)',
    "wind_direction_entry": '風向(16方位)',
    "wind_speed_entry": '風速(m/s)',
    "visibility_entry": '視程(km)',
    "discomfort_index_entry": '不快指数',
}

MAX_FAILED_TIME = 10


def get_weather_data(now_date, place_row):
    """
    GET WEATHER DATA
    """
    pcode = place_row["place_code"]
    big_area = place_row["big_area"]
    mid_area = place_row["mid_area"]
    sml_area = place_row["sml_area"]

    url = _url_generate(now_date, pcode)

    table = None
    failed_time = 0
    while failed_time < MAX_FAILED_TIME:
        try:
            BROWSER.get(url)
            html = BROWSER.find_element_by_tag_name("body").get_attribute(
                'innerHTML')
            bs_obj = BS(html, 'lxml')
            table = bs_obj.find("table", {
                "class": "past_live_point_table"
            }).tbody.extract()
        except Exception as err:
            failed_time += 1

            print("Exception: {0}".format(err))
            print("SLEEP {0} SECONDS AND TRY AGAIN!".format(10 * failed_time))

            time.sleep(10 * failed_time)
        else:
            break
    if failed_time >= MAX_FAILED_TIME:
        print("FAILED URL:{0}".format(url))
        raise Warning("CRAWLER: ERROR!HELP!")

    day_weather, attrs = _extract_weather_data(table)

    data_matrix = np.array(day_weather).T

    col_len = len(list(attrs))
    cols = [[big_area] * col_len, [mid_area] * col_len, [sml_area] * col_len,
            list(attrs)]

    ind_len = data_matrix.shape[0]
    thrhour = [i for i in range(3, 25, 3)]
    inds = [[now_date.strftime("%Y, %m, %d")] * ind_len, thrhour]
    dw_df = DataFrame(data_matrix, columns=cols, index=inds)
    dw_df.index.names = ['date', 'hour']

    return dw_df


def _url_generate(now_date, place_code):
    date_code = _date_encode(now_date)
    url = "http://www.tenki.jp/past/{0}/{1}.html".format(date_code, place_code)
    return url


def _extract_weather_data(table):
    day_weather = []
    for attr, jname in ENTRY_DICT.items():
        first_en = table.find("td", class_=attr)
        en_row = [first_en.get_text()]
        for entry in first_en.next_siblings:
            try:
                en_row.append(entry.get_text())
            except AttributeError:
                continue
        day_weather.append(en_row)
    return day_weather, ENTRY_DICT.values()


def _date_encode(now_date):
    year = str(now_date.year).zfill(2)
    month = str(now_date.month).zfill(2)
    day = str(now_date.day).zfill(2)

    return "{0}/{1}/{2}".format(year, month, day)


def area_code_list():
    columns_attrs = ["big_area", "mid_area", "sml_area", "place_code"]
    df_list = []
    for area_id in range(1, 11):
        table = _get_area_table(area_id)
        area_code = _extract_area_code_data(area_id, table)
        c_df = DataFrame(area_code, columns=columns_attrs)
        df_list.append(c_df)
    area_df = concat(df_list, ignore_index=True)

    area_df.to_csv("area_code.csv", index=False, encoding="cp932")

    return area_df


def _get_area_table(area_id):
    url = "http://www.tenki.jp/past/2018/01/?map_area_id={0}".format(area_id)
    BROWSER.get(url)
    html = BROWSER.find_element_by_tag_name("body").get_attribute('innerHTML')

    bs_obj = BS(html, 'lxml')
    table = bs_obj.find("table", {
        "class": "amedas_pref_point_entries_table"
    }).extract()

    return table


def _extract_area_code_data(area_id, table):
    area_code = []
    tbody = table.tbody
    big_area_name = table['summary'].split(' ')[0]

    for mida in tbody.children:
        try:
            mida_tag = mida.th.a
            smla_tags = mida.td
        except AttributeError:
            continue

        for smla_a in smla_tags.contents:
            try:
                clu = smla_a['href']
                pcode = _get_place_code(clu, area_id)
                area_data = [
                    big_area_name,
                    mida_tag.get_text(),
                    smla_a.get_text(), pcode
                ]
                area_code.append(area_data)
            except TypeError:
                continue

    return area_code


def _get_place_code(clue, area_id):
    map_re = r'map_pref_id=[0-9]?[0-9]'
    jma_re = r'jma_code=\d{5}'
    map_v_re = r'[0-9]?[0-9]'
    jma_v_re = r'\d{5}'
    map_param = re.search(map_re, clue).group(0)
    jam_param = re.search(jma_re, clue).group(0)
    map_pref_id = re.search(map_v_re, map_param).group(0)
    jam_code = re.search(jma_v_re, jam_param).group(0)

    return "{0}/{1}/{2}".format(area_id, map_pref_id, jam_code)


def get_Japan_weather_data():
    ac_df = read_csv("area_code.csv", encoding="cp932")

    awdfs = []
    for index, row in ac_df.iterrows():
        print(datetime.today().strftime("%b %d %Y %H:%M:%S"))
        now_date = date(2016, 1, 1)
        dwdfs = []

        while now_date < date(2017, 6, 1):
            print("{5} ({0} {1} {2} {3}): {4}".format(
                row['big_area'], row['mid_area'], row['sml_area'],
                row['place_code'], now_date, index))
            dw_df = get_weather_data(now_date, row)
            dwdfs.append(dw_df)

            now_date += timedelta(1)
            time.sleep(2 * random())

        aw_df = concat(dwdfs)
        awdfs.append(aw_df)

    pw_df = concat(awdfs, axis=1)
    pw_df.to_csv("place_weather.csv", encoding="cp932")

    return pw_df


if __name__ == '__main__':
    ac_df = read_csv("area_code.csv", encoding="cp932")
    typical_mid_area = ac_df['mid_area'].unique()

    awdfs = []
    count = 0
    for mid_area in typical_mid_area:
        print(datetime.today().strftime("%b %d %Y %H:%M:%S"))
        now_date = date(2016, 1, 1)

        row = ac_df[ac_df['mid_area'] == mid_area].iloc[0]

        dwdfs = []
        while now_date < date(2017, 6, 1):
            print("{5} ({0} {1} {2} {3}): {4}".format(
                row['big_area'], row['mid_area'], row['sml_area'],
                row['place_code'], now_date, count))
            dw_df = get_weather_data(now_date, row)
            dwdfs.append(dw_df)

            now_date += timedelta(1)
            # time.sleep(2 * random())

        aw_df = concat(dwdfs)
        awdfs.append(aw_df)

        count += 1

    pw_df = concat(awdfs, axis=1)
    pw_df.to_csv("place_weather.csv", encoding="cp932")

    print(pw_df)
