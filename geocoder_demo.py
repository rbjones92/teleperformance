# Robert Jones
# 5.9.23
# Python 3.10.7 64-bit
# Geocode PGE data and match with water vendor
import requests
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import func_timeout
import urllib
import time
import numpy as np

pge_filepath = 'C:/Users/Robert.Jones/Central Coast Energy Services, Inc/pge_propensity_data - Documents/csv_docs/propensity_transformed_03.01.23.csv'
water_boundary_file_path = 'C:/Users/Robert.Jones/OneDrive - Central Coast Energy Services, Inc/Documents/Python_Projects/geocode/qgis_layers/water_system_boundaries.shp'
polygons_gdf = gpd.read_file(water_boundary_file_path)
df_eligible = pd.read_csv('eligible_water_systems.csv')

# OLD TEST..failed overnight (computer crashed)
'''
# test an example address (WITHIN TWO WATER COMPANIES)
address = '20200 Big Basin Way, Boulder Creek, CA 95006'
test_file = open('example_address.json')
test_data = json.load(test_file)
coordinates = test_data['result']['addressMatches'][0]['coordinates']
coordinates = Point(coordinates['x'],coordinates['y'])
data = {'Address': ['913 W FRANKLIN ST MONTEREY CA 93940'], 'geometry': coordinates}
point_gdf = gpd.GeoDataFrame(data,crs={'init': 'epsg:4326'})
points_within_polygons = gpd.sjoin(point_gdf.to_crs(crs=3857), polygons_gdf, how='left', op='within')
print(points_within_polygons[['WATER_SYST','WATER_SY_1']])
if len(points_within_polygons.index) > 1:
    df_eligible = pd.read_csv('eligible_water_systems.csv')
    print('multiple water systems found')
    df = pd.DataFrame(points_within_polygons)
    match_list = df.loc[df['WATER_SYST'].isin(df_eligible['System_Num_CA']),'WATER_SYST'].values.tolist()
    if match_list:
        print('found an eligible water system',match_list)
        df = df.loc[df['WATER_SYST'] == match_list[0]]
        print(df)
        water_system_num = df['WATER_SYST'].values[0]
        water_system_name = df['WATER_SY_1'].values[0]
        print(water_system_num,water_system_name)        
    else:
        print('no eligible water systems found')
'''

class GeocodePGE:
    '''
    Geocode pge propensity data with geo.census API
    '''
    def __init__(self):
        self.df = pd.read_csv(pge_filepath)

    def read_and_filter_csv(self):
        print('Len before dropping duplicates: ',len(self.df.index))
        # Drop Duplicates on ACCT_ID keeping most recent date
        self.df = self.df.sort_values(by=['ACCT_ID','date'],ascending=[True,False])
        self.df = self.df.drop_duplicates(subset=['ACCT_ID'],keep='first')
        print('Len after dropping duplicates: ',len(self.df.index))
        # Filter by Counties we Serve
        counties = ['MONTEREY','SANTA CRUZ','SAN MATEO','SAN FRANCISCO']
        self.df = self.df.query('PREM_COUNTY in @counties')
        print('Len after dropping counties',len(self.df.index))
        return self.df
    
    def geocode(self):
        
        def geocoder(ServiceAddress,ServiceCity,ServiceZipCode,ServiceState):
            ServiceAddress = urllib.parse.quote(ServiceAddress)
            ServiceCity = urllib.parse.quote(ServiceCity)
            ServiceZipCode = urllib.parse.quote(ServiceZipCode)
            ServiceState = urllib.parse.quote(ServiceState)

            full_address = f'{ServiceAddress},%20{ServiceCity},%20{ServiceState},%20{ServiceZipCode}'
            base_url = 'https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress?benchmark=Public_AR_Current&vintage=Current_Current&layers=0&format=json&address='            
            
            try:
                def query_api():
                    result = requests.get(base_url+full_address).text
                    time.sleep(.5)
                    return result
                def time_controller(max_wait_time):
                    try:
                        result = func_timeout.func_timeout(max_wait_time,query_api)
                        return result
                    except func_timeout.FunctionTimedOut:
                        print('API call timed out')
                        return 'Api call timed out'
                # If API call last longer than 1.5 seconds, terminate the call
                result = time_controller(1.5)
                result = json.loads(result)
                coordinates = result['result']['addressMatches'][0]['coordinates']

            except Exception as e:
                print(f'Exception Found locating address: {e}')
                print(f'Could not find {full_address}')
                return 'Address Not Found'

            try:
                coordinates = Point(coordinates['x'],coordinates['y'])
            except Exception as e:
                print(f'Exception Found In converting Point to coordinate: {e}')
                print('coordinates could not be converted to point')
                return 'Coordinates Not Found'
            
            data = {'Address': [full_address], 'geometry': coordinates}
            point_gdf = gpd.GeoDataFrame(data,crs='epsg:4326')
            points_within_polygons = gpd.sjoin(point_gdf.to_crs(crs=3857), polygons_gdf, how='left', predicate='within')

            # If you find more than 1 eligible water system
            if len(points_within_polygons.index) > 1:
                print('Multiple water systems found')
                df = pd.DataFrame(points_within_polygons)
                match_list = df.loc[df['WATER_SYST'].isin(df_eligible['System_Num_CA']),'WATER_SYST'].values.tolist()
                # If any of the found water systems are eligible...
                if match_list:
                    # Match on first item of match_list
                    df = df.loc[df['WATER_SYST'] == match_list[0]]
                    water_system_num = df['WATER_SYST'].values[0]
                    water_system_name = df['WATER_SY_1'].values[0]
                    return str(f'{water_system_num}:{water_system_name}')
                # If no water system is eligible
                else:
                    # Return first item in geocode list
                    water_system_num = points_within_polygons['WATER_SYST'].values[0]
                    water_system_name = points_within_polygons['WATER_SY_1'].values[0]
                    print(f'{full_address} : {water_system_num} {water_system_name}')
                    return str(f'{water_system_num}:{water_system_name}')

            else:
                water_system_num = points_within_polygons['WATER_SYST'].values[0]
                water_system_name = points_within_polygons['WATER_SY_1'].values[0]
                # If nan
                if type(water_system_num) == float:
                    return 'Customer Not Found In Water System Boundary'
                else:
                    return str(f'{water_system_num}:{water_system_name}')

        # If needs to be stopped at certain acct number
        # self.df = self.df[self.df['ACCT_ID'] > 9888750895]
        for index, row in self.df.iterrows():
            water_info = geocoder(row['PREM_ADDRESS1'],row['PREM_CITY'],str(row['PREM_ZIP']),row['PREM_STATE'])
            acct_id = int(row['ACCT_ID'])
            data = {'ACCT_ID':[acct_id],
                    'WATER_INFO':[water_info]}
            result_df = pd.DataFrame(data=data)
            print(f'Added {data}')
            result_df.to_csv('matched_vendors.csv',mode='a',index=False,header=False)
        
        return self.df

    def refine_geocode(self):
        geocode_df = pd.read_csv('matched_vendors.csv')
        pge_df = pd.read_csv(pge_filepath)
        merged_df = pd.merge(geocode_df,pge_df,on='ACCT_ID')
        merged_df = merged_df[merged_df['WATER_INFO'] == 'Address Not Found']
        self.df = merged_df[['WATER_INFO','ACCT_ID','PREM_ADDRESS1','PREM_CITY','PREM_ZIP','PREM_STATE']]

        conditions = [self.df['PREM_ADDRESS1'].str.contains('APT'),
                      self.df['PREM_ADDRESS1'].str.contains('UNIT'),
                      self.df['PREM_ADDRESS1'].str.contains('BLDG'),
                      self.df['PREM_ADDRESS1'].str.contains('FL'),
                      self.df['PREM_ADDRESS1'].str.contains('STE'),
                      self.df['PREM_ADDRESS1'].str.contains('RM'),
                      self.df['PREM_ADDRESS1'].str.contains('DEPT')]

        choices = [self.df['PREM_ADDRESS1'].apply(lambda x: x[:x.find('APT')]),
                   self.df['PREM_ADDRESS1'].apply(lambda x: x[:x.find('UNIT')]),
                   self.df['PREM_ADDRESS1'].apply(lambda x: x[:x.find('BLDG')]),
                   self.df['PREM_ADDRESS1'].apply(lambda x: x[:x.find('FL')]),
                   self.df['PREM_ADDRESS1'].apply(lambda x: x[:x.find('STE')]),
                   self.df['PREM_ADDRESS1'].apply(lambda x: x[:x.find('RM')]),
                   self.df['PREM_ADDRESS1'].apply(lambda x: x[:x.find('DEPT')])]
        
        self.df['PREM_ADDRESS1'] = np.select(conditions,choices,default=self.df['PREM_ADDRESS1'])
        
        return self.df
    
    def deduplicate(self):
        # Drop Duplicates (in case missed address both times)
        geocode_df = pd.read_csv('matched_vendors.csv')
        geocode_df = geocode_df.drop_duplicates()
        # Drop Duplicates (remove 'Address Not Found' first if found)
        geocode_df['missed_address_pointer'] = geocode_df['WATER_INFO'].apply(lambda x: 1 if x == 'Address Not Found' else 0)
        geocode_df = geocode_df.sort_values(by=['ACCT_ID','missed_address_pointer'],ascending=[True,True])
        geocode_df = geocode_df.drop_duplicates(subset=['ACCT_ID','missed_address_pointer'],keep='last')
        geocode_df = geocode_df['ACCT_ID','WATER_INFO']
        # Write to csv
        geocode_df.to_csv('matched_vendors_deduplicated.csv',index=False,header=True) 

        return self.df


    def create_final(self):
        geocode_df = pd.read_csv('matched_vendors_deduplicated.csv')
        merged_df = pd.merge(self.df,geocode_df,on='ACCT_ID')
        merged_df['WATER_SYS_NAME'] = merged_df['WATER_INFO'].str.split(':').str[-1]
        merged_df['WATER_INFO'] = merged_df['WATER_INFO'].str.split(':').str[0]
        merged_df['eligible_water_vendor'] = merged_df['WATER_INFO'].apply(lambda x: 'eligible' if x in df_eligible['System_Num_CA'].to_list() else 'not eligible')
        merged_df.to_csv('pge_and_water_info.csv',index=False)

instance = GeocodePGE()
instance.read_and_filter_csv() # EZ deduplicate, filter by our counties
instance.geocode() # First Run Found 92% of records, missed about 10,000
instance.refine_geocode() # Split address data, removing apt,bldg,unit,etc
instance.geocode() # Try again with cleaned data
instance.deduplicate() # Remove any addresses missed twice, update now-found addresses
instance.create_final() # Split column into water sys name and water info. Compare with eligible list.
