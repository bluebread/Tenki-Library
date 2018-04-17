# Tenki-Library
A Japan weather crawler 

OPEN
-------------

```python
dateparse = lambda d: datetime.strptime(d, '%Y, %m, %d')
pw_df = read_csv(
    "fast_place_weather.csv", 
    encoding="cp932",  
    header=[0,1,2,3], 
    index_col=[0,1], 
    parse_dates=[0], 
    date_parser=dateparse)
```
-------------

RRVF_UNION
-------------
si2pw_area_translate.json : This file can help you turn the "mid_area" into the nearest area in fast_place_weather.csv. Note that you must standardize your "area_name" first, and character 'ō' and the words behind '-' are unacceptable.

```python
#Standardize "area_name" in store_info.csv first
std_an_df = DataFrame(
    si_df['area_name'].str.split(' ', 2).tolist(), 
    columns=['big_area', 'mid_area', 'sml_area']
)
std_an_df = std_an_df.drop("sml_area", axis=1)
std_an_df = std_an_df.applymap(lambda area: area.split('-', 1)[0])

# Open si2pw_area_translate.json
with open("si2pw_area_translate.json", "rb") as f:
    content = f.read().decode("utf-8")
    s2p_trans = json.loads(content)
    # ...

# Oepn si_big_area_translate.json
with open("si_big_area_translate.json", "rb") as f:
    content = f.read().decode("utf-8")
    sba_trans = json.loads(content)
    #...
```


SOURCE: http://www.tenki.jp/past/2016/02/01/1/1/47407.html
