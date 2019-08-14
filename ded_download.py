from utils import queryDED
import pandas as pd
import os
import time
import datetime as dt


def get_clusters():
    response = query.listClusters()
    if response["status"] == "ERROR":
        print("ERROR: {}".format(response['message']))
        time.sleep(1)
        return get_clusters()

    temp = pd.io.json.json_normalize(response['data'])
    return temp['alias'].values


def get_devices(clusterAlias):
    response = query.listDevices(clusterAlias=clusterAlias)
    if response['status'] == 'ERROR':
        print("ERROR: {}".format(response['message']))
        time.sleep(1)
        return get_devices(clusterAlias)

    temp = pd.io.json.json_normalize(response['data'])
    return temp[['provider', 'description', 'identifier']]


def select_data(clusterAlias, identifier, period):
    if period['end'] == None: period['end'] = pd.Timestamp.now()
    print("Query for cluster: %s" %(clusterAlias))
    print("Query for device: %s" %(identifier))
    print('Query period: from %s until %s' %(str(period['start']), str(period['end'])))
    df = pd.DataFrame()
    skip = 0
    response = query.listData(clusterAlias=clusterAlias, identifier=identifier, skip=skip)

    if response["status"] == "ERROR":
        print(response["message"])
        return df

    df_seg = pd.io.json.json_normalize(response['data'], sep='_')
    mask = (pd.to_datetime(df_seg['created_at']) >= period['start']) & (pd.to_datetime(df_seg['created_at']) < period['end'])

    # Continue querying data until no data is found within requested period
    while mask.any() or (pd.to_datetime(df_seg.created_at).min() > period['start']):

        print(skip)
        df = df.append(df_seg[mask], sort=False, ignore_index=True)
        skip += 1000
        response = query.listData(clusterAlias=clusterAlias, identifier=identifier, skip=skip)

        if response["status"] == "ERROR":
            print("ERROR: {}".format(response['message']))
            break

        df_seg = pd.io.json.json_normalize(response['data'], sep='_')

        if df_seg.empty:
            print('Stop query loop. No data found in requested period')
            break

        mask = (pd.to_datetime(df_seg['created_at']) >= pd.to_datetime(period['start'])) & (pd.to_datetime(df_seg['created_at']) < pd.to_datetime(period['end']))

    return df


def mergeData (save_path, starttime, endtime, devices, keyword, cluster):
    while True:
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        for _, row in devices.iterrows():
            desc = row.description
            id = row.identifier

            if id + '.csv' not in os.listdir(save_path):
                period = {'start': pd.to_datetime(starttime), 'end': pd.to_datetime(endtime)}
                if keyword in desc:
                    df = select_data(cluster, id, period)
                    while df.empty:
                        df = select_data(cluster, id, period)
                    df = df.drop_duplicates(['_id'])
                    df.to_csv(save_path + id + '.csv')

            else:
                if keyword in desc:
                    df = pd.read_csv(save_path + id + '.csv')
                    while 1:
                        start = df['created_at'].iloc[-1]
                        if pd.to_datetime(start).date() == pd.to_datetime(starttime).date():
                            break
                        period_new = {'start': pd.to_datetime(starttime), 'end': pd.to_datetime(start)}
                        df_new = select_data(cluster, id, period_new)
                        df = pd.concat([df, df_new], axis=0)
                        df = df.drop_duplicates(['_id'])
                        df.to_csv(save_path + id + '.csv')

        if pd.to_datetime(df['created_at'].iloc[-1]).date() == pd.to_datetime(starttime).date():
            break


    return df


if __name__ == "__main__":

    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    query = queryDED(projectId='5c6c0f3d5082d7027bc73eab', webToken='852e6274b9864df1c27daaef1b93f6cda9d68b7d')

    clusters = get_clusters()
    cluster = 'home-6'
    devices = get_devices(clusterAlias=cluster)

    save_path = cluster + '/equipment/'
    starttime = '2019-07-08 00:00:00'; endtime = '2019-08-13 00:00:00'
    starttime_test = '2019-07-04 00:00:00'

    airpurifier = mergeData(save_path, starttime_test, endtime, devices, 'airpurifier', cluster)
    airvisual = mergeData(save_path, starttime_test, endtime, devices, 'AQI', cluster)
    window = mergeData(save_path, starttime, endtime, devices, 'Window', cluster)
    Door = mergeData(save_path, starttime, endtime, devices, 'Door', cluster)
    Motion = mergeData(save_path, starttime, endtime, devices, 'Motion', cluster)
    plug = mergeData(save_path, starttime_test, endtime, devices, 'plug', cluster)
    dehumidifier = mergeData(save_path, starttime_test, endtime, devices, 'dehumidifier', cluster)
    Temperature = mergeData(save_path, starttime_test, endtime, devices, 'Temperature', cluster)




print('Finished')
