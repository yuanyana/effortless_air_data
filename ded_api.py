from utils import queryDED
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import datetime
import matplotlib
import os
import pickle
#matplotlib.use('Qt5Agg')

def get_clusters():
    response = query.listClusters()
    print(response)
    temp = pd.io.json.json_normalize(response['data'])
    return temp['alias'].values

def get_devices(clusterAlias):
    response = query.listDevices(clusterAlias=clusterAlias)
    temp = pd.io.json.json_normalize(response['data'])

    return temp[['provider','description','identifier']]

def select_data(clusterAlias, identifier, period):
    if period['end'] == None: period['end'] = pd.Timestamp.now()
    print("Query for cluster: %s"%(clusterAlias))
    print("Query for device: %s"%(identifier))
    print('Query period: from %s until %s'%(str(period['start']),str(period['end'])))
    df = pd.DataFrame()
    skip = 0
    response = query.listData(clusterAlias=clusterAlias, identifier=identifier, skip=skip)
    if 'data' in response:
        df_seg = pd.io.json.json_normalize(response['data'], sep='_')
        mask = (pd.to_datetime(df_seg['created_at']) >= period['start']) & (pd.to_datetime(df_seg['created_at']) <= period['end'])
        # Continue querying data until no data is found within requested period
        while mask.any() or (pd.to_datetime(df_seg.created_at).min() > period['start']):
            print(skip)
            df = df.append(df_seg[mask],sort=False,ignore_index=True)
            skip += 1000
            response = query.listData(clusterAlias=clusterAlias, identifier=identifier, skip=skip)
            if 'data' in response:
                df_seg = pd.io.json.json_normalize(response['data'], sep='_')
                if not df_seg.empty:
                    mask = (pd.to_datetime(df_seg['created_at']) >= pd.to_datetime(period['start'])) & (pd.to_datetime(df_seg['created_at']) <= pd.to_datetime(period['end']))
                else:
                    print('Stop query loop. No data found in requested period')
                    break
            else:
                print('Stop query loop. Data field is not available')
                break

        # Keep data within requested period
        if not df.empty:
            df['created_at'] = pd.to_datetime(df['created_at'])
            # Remove duplicate timestamps and only keep the first
            df.drop_duplicates(subset='created_at', keep='first', inplace=True)
            df.rename(columns={'created_at': 'ts'}, inplace=True)
            df = df.set_index('ts').sort_index()
            df = df[(df.index >= period['start']) & (df.index <= period['end'])]
    else:
        print('Data field does not exist in initial query')
        for key in response:
            print(response[key])
    df.to_csv(identifier+'.csv')
    print(df)
    return df

def get_MiSensorHT_data(clusterAlias, identifier, period, resample=True):
    df = select_data(clusterAlias, identifier, period)

    if not df.empty:
        # Keep data with valid Humidity / Temp value
        df = df[df['type']=='HUMTEMP_STATUS']
        # Keep humidity, temperature and voltage columns
        df = df[['data_humidity','data_temperature','data_voltage']]
        # Interpolate and resample the data
        if resample:
            df = df.resample('1S').mean().interpolate()
            df = df.resample('1T').ffill()

        #rename column names
        columns_new = {col:col.replace('data','ht') for col in df.columns}
        df.rename(columns=columns_new,inplace=True)
    else:
        print("No data received for: %s"%(identifier))
    return df

def get_AirPurDehum_data(clusterAlias, identifier, period, resample=True):
    df = select_data(clusterAlias, identifier, period)

    # if not df.empty:
    #     df = df[df['type'] == 'AC_STATUS']

        #Convert categorial data into integer codes
        # df['data_om'].replace(['s','t'],[4,5], inplace = True ) #This operation replases 's' and 't' with 4 and 5, respectively
        # df['data_mode'].replace(['M','P','A','B'], [0, 1, 2, 3],inplace=True)
        # df['data_uil'].replace(1,2)
        # df['data_uil'].replace('D', 1)
        # df['data_func'].replace(['P','PH'],[0,1], inplace=True)
        # df['data_cl'].replace([False, True], [0, 1], inplace = True)  # All values cl are False?

        # df = df[df.columns[df.columns.str.contains('data_')]]

        # df = df.select_dtypes(['number']) #For the moment we don't use non numeric data
        # df = df.drop(columns=['data_err']) #Just to remove high values for better visualization

        # if resample:
        #     df = df.resample('1T').ffill()

        #rename column names
        # columns_new = {col:col.replace('data','apdh') for col in df.columns}
        # df.rename(columns=columns_new,inplace=True)
    # else:
    #     print("No data received for: %s" % (identifier))
    return df

def get_SmartPlug_data_HT(clusterAlias, identifier, period, resample=True):
    df = select_data(clusterAlias, identifier, period)

    if not df.empty:
        df = df[df['type'] == 'PWR_STATUS']

        df = df[df.columns[df.columns.str.contains('data_')]]
        #df = df.drop(columns=['data_humidity','data_temperature'])
        df = df.select_dtypes(['number'])
        if resample:
            df = df.resample('1T').ffill()

        #rename column names
        columns_new = {col:col.replace('data','sp') for col in df.columns}
        df.rename(columns=columns_new,inplace=True)
    else:
        print("No data received for: %s"%(identifier))
    return df

def get_SmartPlug_data_AC(clusterAlias, identifier, period, resample=True):
    df = select_data(clusterAlias, identifier, period)

    if not df.empty:
        df = df[df['type'] == 'PWR_STATUS']

        df = df[df.columns[df.columns.str.contains('data_')]]
        #df = df.drop(columns=['data_humidity','data_temperature'])
        df = df.select_dtypes(['number'])

        if resample:
            df = df.resample('1T').ffill()

        #rename column names
        columns_new = {col:col.replace('data','sp') for col in df.columns}
        df.rename(columns=columns_new,inplace=True)
    else:
        print("No data received for: %s"%(identifier))
    return df

def get_DoorStatus_data(clusterAlias, identifier, period, resample=True):
    df = select_data(clusterAlias, identifier, period)

    if not df.empty:
        df = df[df['type'] == 'OPEN/CLOSE_EVENT']

        df = df[['data_status']] #For the moment we don't use no_close, voltage

        # Remove rows with nan's caused by no close event
        df = df.dropna()
        # Replace status labels with a binary value
        df['data_status'] = df['data_status'].replace(['open','close'],[1,0])

        if resample:
            df = df.resample('1T').ffill()

        #rename column names
        columns_new = {col:col.replace('data','ds') for col in df.columns}
        df.rename(columns=columns_new,inplace=True)
    else:
        print("No data received for: %s"%(identifier))
    return df

def get_WindowStatus_data(clusterAlias, identifier, period, resample=True):
    df = select_data(clusterAlias, identifier, period)

    if not df.empty:
        df = df[df['type'] == 'OPEN/CLOSE_EVENT']

        df = df[['data_status']] #For the moment we don't use no_close, voltage

        # Remove rows with nan's caused by no close event
        df = df.dropna()
        # Replace status labels with a binary value
        df['data_status'] = df['data_status'].replace(['open','close'],[1,0])

        if len(df) == 1:
            df.resample('1T').bfill()
        else:
            if resample:
                df = df.resample('1T').ffill()

        #rename column names
        columns_new = {col:col.replace('data','ws') for col in df.columns}
        df.rename(columns=columns_new,inplace=True)
    else:
        print("No data received for: %s"%(identifier))

    return df

def get_Motion_data(clusterAlias, identifier, period):
    df = select_data(clusterAlias, identifier, period)

    if not df.empty:
        df = df[df['type'] == 'MOTION_EVENT']

        df = df[['data_status']]

        # Remove rows with nan's caused by no motion event
        df = df.dropna()
        # Replace status labels with a binary value
        df['data_status'] = df['data_status'].replace('motion',1)
        df.index = df.index.round('1s')

        index = pd.date_range(start = df.index[0], end = df.index[-1], freq='1T')
        df_motion = pd.DataFrame(index=index)
        df_motion['motion'] = 0
        for idx in df.index: df_motion.loc[idx]=1

        df_motion.rename(columns={'motion': 'm_motion'}, inplace=True)
    else:
        df_motion = pd.DataFrame()
        print("No data received for: %s" % (identifier))
    return df_motion

def estimate_MotionPeriod(clusterAlias, identifier, period):
    df = select_data(clusterAlias, identifier, period)

    if not df.empty:
        df = df[df['type'] == 'MOTION_EVENT']
        df = df[['data_no_motion','data_status']]

        prev = 'no_motion'
        motion_periods = []
        for _,row in df.iterrows():
            if row['data_status'] == 'motion' and prev == 'no_motion':
                ts_str = row.name
                prev = 'motion'
            if (not np.isnan(row['data_no_motion'])) and prev == 'motion':
                ts_end = row.name - datetime.timedelta(seconds=row['data_no_motion'])
                if ts_end < ts_str:
                    ts_end = ts_str
                print('%s %s' % (ts_str, ts_end))
                motion_periods.append((ts_str, ts_end))
                prev = 'no_motion'

        index = pd.date_range(start = motion_periods[0][0].floor('1T'), end = motion_periods[-1][1].ceil('1T'), freq='1T')
        df_motion = pd.DataFrame(index=index)
        df_motion['motion'] = 0
        for motion_period in motion_periods:
            df_motion[(df_motion.index > motion_period[0]) & (df_motion.index <= motion_period[1])] = 1
            if motion_period[0] == motion_period[1]:
                df_motion.iloc[df_motion.index.get_loc(motion_period[0].round('1T'))]=1

        # Get motion events where elapsed time since last motion events are not ascending and not separated with motion event
        diff = df['data_no_motion'].reset_index(drop=True).diff()
        idxs = df['data_no_motion'].iloc[np.asarray(diff.index[(diff <= 0) == True].tolist())]
        for k,v in idxs.items():
            df_motion.iloc[df_motion.index.get_loc((k - datetime.timedelta(seconds=v)).round('1T'))]= 1

        #rename column names
        df_motion.rename(columns={'motion':'mp_motion'},inplace=True)
    else:
        df_motion = pd.DataFrame()
        print("No data received for: %s" % (identifier))
    return df_motion


def get_AirVisual_data(clusterAlias, identifier, period, resample=True):

    def create_df(dataframe, group, col_drop):
        df = dataframe[list(dataframe.filter(regex=group))].copy()
        print(len(df))
        # remove rows with duplicate timestamps
        df = df[~df[group + '_ts'].duplicated(keep='first')]
        print(len(df))
        # shorten columns names
        df = df.rename(columns={col: col[len(group + '_'):] for col in df.columns})
        # remove unnecessary columns
        df = df.drop(columns=col_drop)
        # index becomes timestamp
        df['ts'] = pd.to_datetime(df['ts'])
        df = df.set_index('ts')
        df = df.apply(pd.to_numeric)
        if resample:
            # Resample
            # for interpolation method see https://towardsdatascience.com/preprocessing-iot-data-linear-resampling-dde750910531
            df = df.resample('1S').mean().interpolate()
            df = df.resample('1T').ffill()

        columns_new = {col: col.replace('data', 'av') for col in df.columns}
        df.rename(columns=columns_new, inplace=True)

        return df

    df_current = pd.DataFrame()
    df_outdoor_station = pd.DataFrame()
    df_outdoor_weather = pd.DataFrame()

    df = select_data(clusterAlias, identifier, period)

    if not df.empty:
        df = df[df['type'] == 'AQI_STATUS']

        df = df[df.columns[df.columns.str.contains('data_')]]

        # Created dataframe with 'current' measurements
        df_current = create_df(dataframe = df, group='data_current',col_drop=['errors'])
        columns_new = {col: 'cur_' + col for col in df_current.columns}
        df_current.rename(columns=columns_new, inplace=True)

        # Created dataframe with 'outdoor_station' measurements
        df_outdoor_station = create_df(dataframe=df, group='data_outdoor_station', col_drop=['maincn','mainus'])
        columns_new = {col: 'os_' + col for col in df_outdoor_station.columns}
        df_outdoor_station.rename(columns=columns_new, inplace=True)

        # Created dataframe with 'outdoor_weather' measurements
        col_drop = ['ic']
        columns_test = ['data_outdoor_weather___v','data_outdoor_weather_createdAt','data_outdoor_weather_updatedAt', 'data_outdoor_weather_station_id']
        col_drop.extend([col[21:] for col in columns_test if col in df.columns])

        df_outdoor_weather = create_df(dataframe=df, group='data_outdoor_weather', col_drop=col_drop)
        columns_new = {col: 'cur_' + col for col in df_outdoor_weather.columns}
        df_outdoor_weather.rename(columns=columns_new, inplace=True)
    else:
        print("No data received for: %s"%(identifier))

    return df_current, df_outdoor_station, df_outdoor_weather

if __name__ == "__main__":

    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'data')

    query = queryDED(projectId='5c6c0f3d5082d7027bc73eab', webToken='852e6274b9864df1c27daaef1b93f6cda9d68b7d')

    clusters = get_clusters()
    cluster = 'home-2'
    devices = get_devices(clusterAlias=cluster)

    period = {'start': pd.to_datetime('2019-06-01 00:00:00'), 'end': pd.to_datetime('2019-07-12 00:00:00')}

    d = {}
    for _, row in devices.iterrows():
        desc = row.description
        id = row.identifier
        if 'airpurifier' in desc:
            print('ap')
            data = get_AirPurDehum_data(clusterAlias=cluster, identifier=id, period=period, resample=False)
        if 'AQI' in desc:
            print('av')
            data, av_od_station, av_od_weather = get_AirVisual_data(clusterAlias=cluster, identifier=id, period=period)
        if 'Gateway' in desc:
            data = []
        if 'Window'in desc:
            print('w')
            data = get_WindowStatus_data(clusterAlias=cluster, identifier=id, period=period)
        if 'Door' in desc:
            print('D')
            data = get_DoorStatus_data(clusterAlias=cluster, identifier=id, period=period)
        if 'Motion' in desc:
            print('M')
            data = estimate_MotionPeriod(clusterAlias=cluster, identifier=id, period=period)
        if 'plug' in desc and 'dehumidifier' in desc:
            print('dp')
            data = get_SmartPlug_data_HT(clusterAlias=cluster, identifier=id, period=period)
        if 'plug 'in desc and 'AC' in desc:
            print('ap')
            data = get_SmartPlug_data_AC(clusterAlias=cluster, identifier=id, period=period)
        if 'Temperature' in desc:
            print('T')
            data = get_MiSensorHT_data(clusterAlias=cluster, identifier=id, period=period)
        else:
            'Unknown device'

        # #Store aggregated data into a data frame
        # if isinstance(data,pd.DataFrame):
        #     d[id] = {'provider': row.provider, 'description': desc, 'data':data}

    # # plot the data
    # for device in d:
    #     d[device]['data'].plot()
    #
    # #d.to_pickle(os.path.join(file_path,'d.pkl'))

    # # Store data (serialize)
    # with open(os.path.join(file_path,'d.pkl'), 'wb') as handle:
    #     pickle.dump(d, handle, protocol=pickle.HIGHEST_PROTOCOL)
    #
    # # # Load data (deserialize)
    # # with open(os.path.join(file_path,'d.pkl'), 'rb') as handle:
    # #     unserialized_data = pickle.load(handle)

    print('Finished')