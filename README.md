# Tenki-Library
A Japan weather crawler 

OPEN
-------------
dateparse = lambda d: datetime.strptime(d, '%Y, %m, %d')
pw_df = read_csv("place_weather.csv", encoding="cp932",  header=[0,1,2,3], index_col=[0,1], parse_dates=[0], date_parser=dateparse)
-------------

SOURCE: http://www.tenki.jp/past/2016/02/01/1/1/47407.html
