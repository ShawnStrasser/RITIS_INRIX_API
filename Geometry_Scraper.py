import requests
import json
import uuid
import pandas as pd
from typing import List, Tuple
import warnings
import getpass

class GeometryScraper:
    def __init__(self, cookie: str = None):
        if cookie is None:
            cookie = getpass.getpass(prompt='Enter your cookie: ')
        self.cookie = cookie
        self.headers = {'Cookie': cookie}
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    def get_segments(self, locations: List[Tuple[float, float]], buffer_size: int) -> List[str]:
        url = 'https://pda.ritis.org/api/intersecting_geometry/'
        geometries = {
            "exclude": [],
            "include": [
                {
                    "type": "circle",
                    "radius": buffer_size,
                    "lat": lat,
                    "lng": lon,
                    "leafletID": i
                }
                for i, (lat, lon) in enumerate(locations)
            ]
        }
        data = {
            "geometries": geometries,
            "states": [],
            "road_classes": [],
            "uuid": str(uuid.uuid4()),
            "datasource": "inrix_xd",
            "segment_type": "XD",
            "include_full_tmc_network": False,
            "countryCode": "USA",
            "tool_id": "3"
        }
        response = requests.post(url, headers=self.headers, data=json.dumps(data), verify=False)
        response.raise_for_status()
        return response.json()['tmcList']

    def get_geometry(self, tmc_list: List[str]) -> dict:
        url = 'https://pda.ritis.org/api/xd_coordinates/'
        payload = {
            "datasource_id": "inrix_xd",
            "atlas_version_id": 13,
            "geo_limit": 7000,
            "include_full_tmc_network": True,
            "country_code": None,
            "tool_id": "3",
            "segments": tmc_list,
            "very": False
        }
        response = requests.post(url, headers=self.headers, json=payload, verify=False)
        response.raise_for_status()
        return response.json()

    def extract_data_to_dataframe(self, json_data: dict) -> pd.DataFrame:
        tmcs_data = json_data['tmcs']
        df = pd.DataFrame(tmcs_data)
        
        for feature in json_data['geojson']['features']:
            seg_id = feature['id']
            coordinates = feature['geometry']['coordinates'][0]
            idx = df.index[df['segID'] == seg_id].tolist()
            if idx:
                df.at[idx[0], 'coordinates'] = str(coordinates)
        
        return df

    def process_locations(self, locations: List[Tuple[float, float]], buffer_size: int) -> pd.DataFrame:
        print(f"Processing {len(locations)} locations in batches of 500")
        
        batch_size = 500
        all_dataframes = []

        for i in range(0, len(locations), batch_size):
            batch_locations = locations[i:i+batch_size]
            
            print(f"Processing batch {i//batch_size + 1}")
            batch_segments = self.get_segments(batch_locations, buffer_size)
            batch_geometry_data = self.get_geometry(batch_segments)
            batch_df = self.extract_data_to_dataframe(batch_geometry_data)
            all_dataframes.append(batch_df)

        print("Combining all batches into a single DataFrame")
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        return final_df