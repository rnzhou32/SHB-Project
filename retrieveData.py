#this code need to run under the lll-book because it needs the utility package to connect to link lab influx database

import pandas
import pandas as pd
import numpy as np
from datetime import datetime
import utility as util

# train data
train_meetings = [
    (datetime(2022, 9, 1, 16, 0, 0, 0), datetime(2022, 9, 1, 16, 50, 0, 0), 1), #brad
    (datetime(2022, 9, 8, 16, 0, 0, 0), datetime(2022, 9, 8, 16, 50, 0, 0), 1), #brad
    (datetime(2022, 9, 15, 16, 0, 0, 0), datetime(2022, 9, 15, 16, 50, 0, 0), 1), #brad
    (datetime(2022, 8, 29, 12, 0, 0, 0), datetime(2022, 8, 29, 12, 50, 0, 0), 2), #ben
    (datetime(2022, 9, 5, 12, 0, 0, 0), datetime(2022, 9, 5, 12, 50, 0, 0), 2), #ben
    (datetime(2022, 9, 12, 12, 0, 0, 0), datetime(2022, 9, 12, 12, 50, 0, 0), 2), #ben
    (datetime(2022, 8, 29, 17, 0, 0, 0), datetime(2022, 8, 29, 18, 20, 0, 0), 3),  # CAR
    (datetime(2022, 8, 31, 17, 0, 0, 0), datetime(2022, 8, 31, 18, 20, 0, 0), 3),  # CAR
    (datetime(2022, 9, 5, 17, 0, 0, 0), datetime(2022, 9, 5, 18, 20, 0, 0), 3),  # CAR
    (datetime(2022, 8, 31, 12, 0, 0, 0), datetime(2022, 8, 31, 12, 50, 0, 0), 4),  # Barnes
    (datetime(2022, 8, 31, 12, 0, 0, 0), datetime(2022, 8, 31, 12, 50, 0, 0), 4),  # Barnes
    (datetime(2022, 9, 7, 12, 0, 0, 0), datetime(2022, 9, 7, 12, 50, 0, 0), 4),  # Barnes
    (datetime(2022, 9, 2, 10, 30, 0, 0), datetime(2022, 9, 2, 12, 20, 0, 0), 5), #dsa
    (datetime(2022,8,29,14,30,0,0),datetime(2022,8,29,16,30,0,0),0), #no
    (datetime(2022,8,30,13,0 ,0,0),datetime(2022,8,30,16,30,0,0),0) #no
  # (datetime(2022,9,6 ,12,45,0,0),datetime(2022,9,6 ,14,0 ,0,0)),
  # (datetime(2022,9,6 ,16,30,0,0),datetime(2022,9,6 ,17,0 ,0,0)),
  # (datetime(2022,9,8 ,12,45,0,0),datetime(2022,9,8 ,13,45,0,0)),
  # (datetime(2022,9,9 ,13,0 ,0,0),datetime(2022,9,9 ,14,40,0,0)),
]

#test data
test_meetings = [
    (datetime(2022,9,22,16,0,0,0),datetime(2022,9,22,16,50,0,0), 1),#brad
    (datetime(2022,9,19,12,0,0,0),datetime(2022,9,19,12,50,0,0), 2),#ben
    (datetime(2022,9,7,17,0,0,0),datetime(2022,9,7,18,20,0,0), 3),#car
    (datetime(2022,9,14,12,0,0,0),datetime(2022,9,14,12,50,0,0), 4), #Barnes
    (datetime(2022,9,7,10,30,0,0),datetime(2022,9,7,12,20,0,0), 5),#dsa
    (datetime(2022, 9, 1, 12, 30, 0, 0), datetime(2022, 9, 1, 13, 30, 0, 0), 0), #no
    (datetime(2022, 9, 5, 15, 35, 0, 0), datetime(2022, 9, 5, 16, 35, 0, 0), 0)  #no
]

all_meetings = train_meetings

def getByGrid(type, sensor):
    df = pd.read_csv("book_with_grids.csv")

    df = df[(df['grid'] == 136) | (df['grid'] == 137) | (df['grid'] == 156) | (df['grid'] == 157)]
    df1 = df[(df['type'] == 'awair_element')]
    # devices = list(df[df['type'] == type]['device_id'])
    # devices.sort()  # that way you get the same order

    data = pandas.DataFrame()

    for meeting in all_meetings:
        for s, e in meeting:
            rs = util.get_lfdf("spl_a", s, e, list(df1['device_id']))
            X_Y = {}
            for r in rs:
                for key in r:
                    if key['value'] is None:
                        continue
                    X_Y.update({datetime.fromisoformat(key['time'].split(".")[0].replace("T", " ").replace("Z", "")): key['value']})
            df = pd.DataFrame.from_dict(X_Y, orient='index', columns=['value'])
            df = df.resample("10S").mean()
            data = data.append(df)
    data = data.sort_index()
    return data

def getData(type, sensor, location):
    # necessary steps for retreiving the data
    df = pd.read_csv('book_with_grids.csv')
    devices = list(df[df['type'] == type]['device_id'])
    devices.sort()

    data = pandas.DataFrame()
    for s, e, i in all_meetings:
        rs = util.get_cache_rs(sensor, s, e, devices)
        X_Y = {}
        for r in rs:
            for key in r:
                if key['value'] is None:
                    continue
                # for each line that meet the location specific, we take them out and put into pandas
                if key['location_specific'] == location:
                    X_Y.update({datetime.fromisoformat(
                        key['time'].split(".")[0].replace("T", " ").replace("Z", "")): key['value']})
        # restruct the datafram
        df = pd.DataFrame.from_dict(X_Y, orient='index', columns=['value'])
        # resample them into certain time
        df = df.resample("10S").mean()
        data = data.append(df)
    data = data.sort_index()
    return data

def getMotion():
    df = pd.read_csv('book_with_grids.csv')
    devices = list(df[df['type'] == "dual_motion"]['device_id'])
    devices.sort()  # that way you get the same order
    data = pandas.DataFrame()
    for s, e, i in all_meetings:
        rs = util.get_cache_rs("PIR Status", s, e, devices)
        X_Y = {}
        for r in rs:
            for key in r:
                if key['value'] is None:
                    continue
                if key['location_specific'] == "241 Olsson":
                    X_Y.update({datetime.fromisoformat(key['time'].split(".")[0].replace("T", " ").replace("Z", "")): key['value']})
        df = pd.DataFrame.from_dict(X_Y, orient='index', columns=['value'])
        df = df.resample("10S").mean()
        df['feature'] = i
        for index in df.index:
            q = df._get_value(index, 'value')
            if np.isnan(q):
                df.at[index, 'value'] = prev
            else:
                prev = q
        data = data.append(df)
    data = data.sort_index()
    return data


def main():

    # getByGrid("awair_element", "spl_a")
    # ben_spla = getByGrid().rename(columns={'value': 'ben_spla'})

    motion = getMotion().rename(columns={'value': 'brad_office'})
    co2 = getData("awair_element", "co2_ppm", "211 Olsson").rename(columns={'value': 'co2'})
    humidity = getData("awair_element", "Humidity_%", "211 Olsson").rename(columns={'value': 'humidity'})
    temp = getData("awair_element", "Temperature_°C", "211 Olsson").rename(columns={'value': 'temp'})
    pm25 = getData("awair_element", "pm2.5_μg/m3", "211 Olsson").rename(columns={'value': 'pm25'})
    voc = getData("awair_element", "voc_ppb", "211 Olsson").rename(columns={'value': 'voc'})
    illumination = getData("awair_element", "Illumination_lx", "211 Olsson").rename(columns={'value': 'illumination'})
    spla = getData("awair_element", "spl_a", "211 Olsson").rename(columns={'value': 'spla'})
    ben_illumination = getData("light_level", "Illumination_lx", "249 Olsson").rename(columns={'value': 'ben_illumination'})
    ben_spla = getData("awair_element", "spl_a", "240 Olsson").rename(columns={'value': 'ben_spla'})


    tol = pd.Timedelta('1 hour')
    all = pd.merge_asof(left=co2,right=humidity,right_index=True,left_index=True,direction='nearest',tolerance=tol)
    all = pd.merge_asof(left=all, right=temp, right_index=True, left_index=True, direction='nearest', tolerance=tol)
    all = pd.merge_asof(left=all, right=pm25, right_index=True, left_index=True, direction='nearest', tolerance=tol)
    all = pd.merge_asof(left=all, right=voc, right_index=True, left_index=True, direction='nearest', tolerance=tol)
    all = pd.merge_asof(left=all, right=illumination, right_index=True, left_index=True, direction='nearest', tolerance=tol)
    all = pd.merge_asof(left=all, right=spla, right_index=True, left_index=True, direction='nearest', tolerance=tol)
    # all = pd.merge_asof(left=all, right=ben_illumination, right_index=True, left_index=True, direction='nearest', tolerance=tol)
    # all = pd.merge_asof(left=all, right=ben_spla, right_index=True, left_index=True, direction='nearest', tolerance=tol)
    all = pd.merge_asof(left=all, right=motion, right_index=True, left_index=True, direction='nearest', tolerance=tol)

    # all = all.drop('brad_office', axis=1)
    all.to_csv("trainData_brad.csv")


if __name__ == '__main__':
    main()
