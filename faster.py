"""
FASTER CRAWLER
"""
from datetime import date, timedelta, datetime
import time
from random import random
from urllib.request import urlopen

from bs4 import BeautifulSoup as BS
import numpy as np
from pandas import DataFrame, concat, read_csv

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

MAX_FAILED_TIME = 5


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
            js = urlopen(url).read()
            table_xml = eval(js[15:-1].decode('utf-8'))
            bs_obj = BS(table_xml, 'lxml')
            table = bs_obj.find("table", {
                "class": "past_live_point_table"
            }).extract()
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

#    col_len = len(list(attrs))
#    cols = [[big_area] * col_len, [mid_area] * col_len, [sml_area] * col_len,
#            list(attrs)]
    cols = list(attrs)

    ind_len = data_matrix.shape[0]
    thrhour = [i for i in range(3, 25, 3)]
#    inds = [[now_date.strftime("%Y, %m, %d")] * ind_len, thrhour]
#    dw_df = DataFrame(data_matrix, columns=cols, index=inds)
#    dw_df.index.names = ['date', 'hour']
    dw_df = DataFrame(data_matrix, columns=cols)
    
    time_df = DataFrame(
            {"date": [now_date.strftime("%Y, %m, %d")] * ind_len,
             "hour": thrhour})
    
    area_df = DataFrame(
            {"big_area": [big_area] * ind_len,
             "mid_area": [mid_area] * ind_len,
             "sml_area": [sml_area] * ind_len})
    dw_df = concat([time_df, dw_df, area_df], axis=1)
        
    return dw_df


def _url_generate(now_date, place_code):
    date_code = _date_encode(now_date)
    url = "http://az416740.vo.msecnd.net/component/static_api/past/live/{0}/point_{1}.js".format(
        date_code, place_code[-5:])
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
    year = str(now_date.year)
    month = str(now_date.month).zfill(2)
    day = str(now_date.day).zfill(2)

    return "{0}/{1}/{2}".format(year, month, day)


if __name__ == '__main__':
    ac_df = read_csv("area_code.csv", encoding="cp932")

    awdfs = []
    for index, row in ac_df.iterrows():
        print(datetime.today().strftime("%b %d %Y %H:%M:%S"))
        now_date = date(2016, 1, 1)

        dwdfs = []
        while now_date < date(2016, 1, 3):
            print("{5} ({0} {1} {2} {3}): {4}".format(
                row['big_area'], row['mid_area'], row['sml_area'],
                row['place_code'], now_date, index))
            dw_df = get_weather_data(now_date, row)
            dwdfs.append(dw_df)

            now_date += timedelta(1)
            # time.sleep(1 * random())

        aw_df = concat(dwdfs)
        awdfs.append(aw_df)

    pw_df = concat(awdfs)
    pw_df.to_csv("fast_place_weather.csv", encoding="cp932", index=False)
    

    print(pw_df)
