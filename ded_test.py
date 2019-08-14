from utils import queryDED
import pandas as pd
import os
import time


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


def select_data(clusterAlias, identifier, period, save_path):
    if period['end'] == None: period['end'] = pd.Timestamp.now()
    print("Query for cluster: %s" %(clusterAlias))
    print("Query for device: %s" %(identifier))
    print('Query period: from %s until %s' %(str(period['start']), str(period['end'])))
    df = pd.DataFrame()
    skip = 0
    response = query.listData(clusterAlias=clusterAlias, identifier=identifier, skip=skip)
    print('response:', response)

    if response["status"] == "ERROR":
        print(response["message"])
        return df

    df_seg = pd.io.json.json_normalize(response['data'], sep='_')
    print('df_seg:', df_seg)
    mask = (pd.to_datetime(df_seg['created_at']) >= period['start']) & (pd.to_datetime(df_seg['created_at']) <= period['end'])

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

        mask = (pd.to_datetime(df_seg['created_at']) >= pd.to_datetime(period['start'])) & (pd.to_datetime(df_seg['created_at']) <= pd.to_datetime(period['end']))

    # df.to_csv(save_path + identifier + '.csv')

    return df


if __name__ == "__main__":

    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    query = queryDED(projectId='5c6c0f3d5082d7027bc73eab', webToken='852e6274b9864df1c27daaef1b93f6cda9d68b7d')

    clusters = get_clusters()
    cluster = 'home-6'
    devices = get_devices(clusterAlias=cluster)

    save_path = cluster + '/equipment/'

    if not os.path.exists(save_path):
        os.makedirs(save_path)
        period = {'start': pd.to_datetime('2019-07-01 00:00:00'), 'end': pd.to_datetime('2019-08-12 00:00:00')}

        for _, row in devices.iterrows():
            desc = row.description
            id = row.identifier
            if 'airpurifier' in desc:
                df = select_data(cluster, id, period, save_path)
                print(df)
            # if 'AQI' in desc:
            #     df = select_data(cluster, id, period, save_path)
            # if 'Window' in desc:
            #     df = select_data(cluster, id, period, save_path)
            # if 'Door' in desc:
            #     df = select_data(cluster, id, period, save_path)
            # if 'Motion' in desc:
            #     df = select_data(cluster, id, period, save_path)
            # if 'plug' in desc and 'dehumidifier' in desc:
            #     df = select_data(cluster, id, period, save_path)
            # if 'plug ' in desc and 'AC' in desc:
            #     df = select_data(cluster, id, period, save_path)
            # if 'Temperature' in desc:
            #     df = select_data(cluster, id, period, save_path)

    # else:
    #     for _, row in devices.iterrows():
    #         desc = row.description
    #         id = row.identifier
    #         if 'airpurifier' in desc:
    #             df = pd.read_csv(save_path + id + '.csv')
    #             if not df.empty:
    #                 start = df['created_at'][0]
    #                 period = {'start': pd.to_datetime('2019-07-01 00:00:00'), 'end': pd.to_datetime(start)}
    #                 df = select_data(cluster, id, period, save_path)
    #             else:
    # #                 start = av['created_at'][0]
    # #                 period = {'start': pd.to_datetime('2019-06-01 00:00:00'), 'end': pd.to_datetime(start)}
    # #                 df = select_data(cluster, id, period, save_path+str(n))
    # #                 data = pd.read_csv(save_path+id+'.csv')
    # #                 data = pd.concat([ data, df ], axis=0)



print('Finished')