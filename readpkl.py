import pickle
import os
import pandas as pd
import datetime as dt
import time


# AC2729/10
data_file = os.getcwd()+'/home1/equipment'
AC = pd.DataFrame()
ACdata = pd.read_csv(data_file+'/'+'E8:C1:D7:05:E5:38.csv')

AC['AC2729-Mode'] = ACdata['data_mode']
AC['AC2729-FanSpeed'] = ACdata['data_om']
AC['AC2729-Power'] = ACdata['data_pwr']
AC['AC2729-PM25'] = ACdata['data_pm25']
AC.set_index(ACdata['ts'], inplace= True)
print(AC)

#Air Visual
AV = pd.DataFrame()
AVdata = pd.read_csv(data_file+'/'+'54:C9:DF:D5:22:4E.csv')

AV['AirVisual-IndoorCO2'] = AVdata['data_current_co']
AV['AirVisual-IndoorTem'] = AVdata['data_current_tp']
AV['AirVisual-IndoorPM25'] = AVdata['data_current_p2']
AV['AirVisual-IndoorHum'] = AVdata['data_current_hm']
AV['AirVisual-StationP2Conc'] = AVdata['data_outdoor_station_p2_conc']
AV['AirVisual-StationP2Aqicn'] = AVdata['data_outdoor_station_p2_aqicn']
AV['AirVisual-StationP2'] = AVdata['data_outdoor_station_p2_aqius']
AV['AirVisual-StationMaincn'] = AVdata['data_outdoor_station_maincn']
AV['AirVisual-StationAqius'] = AVdata['data_outdoor_station_aqius']
AV.set_index(AVdata['ts'], inplace=True)
print(AV)

# Door-Magnet
DM = pd.DataFrame()
DMdata = pd.read_csv(data_file+'/'+'158d0002a63d2b.csv')

DM['DoorMagnetState'] = DMdata['data_status']
DM.set_index(DMdata['ts'], inplace=True)
print(DM)

# Windows-Magnet
WM = pd.DataFrame()
WMdata = pd.read_csv(data_file+'/'+'158d00028e01f2.csv')

WM['WindowMagnetState'] = WMdata['data_status']
print(WMdata)
WM.set_index(WMdata['ts'], inplace=True)
print(WM)

# MiMotion
MM = pd.DataFrame()
MMdata = pd.read_csv(data_file+'/'+'158d0002c53624.csv')

MM['MIMotion-State'] = MMdata['data_status']
MM.set_index(MMdata['ts'], inplace=True)
print(MM)

# MHT
MHT = pd.DataFrame()
MHTdata = pd.read_csv(data_file+'/'+'158d000358ea39.csv')
MHT['MiSensorHT-Humidity'] = MHTdata['data_humidity']
MHT['MiSensorHT-Temperature'] = MHTdata['data_temperature']
MHT.set_index(MHTdata['ts'], inplace=True)


# ACP
ACP = pd.DataFrame()
ACPdata = pd.read_csv(data_file+'/'+'158d00036be0cb.csv')
ACP['ACPlug-Power'] = ACPdata['data_load_power']
ACP['ACPlug-Use'] = ACPdata['data_inuse']
ACP['ACPlug-Status'] = ACPdata['data_status']
ACP.set_index(ACPdata['ts'], inplace=True)

# DP
DP = pd.DataFrame()
DPdata = pd.read_csv(data_file+'/'+'158d000343331e.csv')
DP['DehumPlug-Power'] = DPdata['data_load_power']
DP['DehumPlug-Use'] = DPdata['data_inuse']
DP['DehumPlug-Status'] = DPdata['data_status']
DP.set_index(DPdata['ts'], inplace=True)

frame = [AC, AV, DM, WM, MM, MHT, ACP, DP]
result = pd.concat(frame, axis=1)
result.to_csv('a.csv')


result = pd.read_csv('a.csv')
result.rename(columns={'Unnamed: 0':'ts'}, inplace=True)
print(result['ts'])
result['Time'] = pd.to_datetime(result['ts']).dt.date

print(result)
for name, group in result.groupby(result['Time']):
    group = group.drop('Time', axis= 1)
    group.to_csv(os.getcwd()+'/home1/date/'+str(name)+'.csv')





