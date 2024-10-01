import requests
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
import os

class INRIX_Downloader:
    def __init__(self, app_id_path, hash_token_path, token_path, segments_path, verbose=1):
        self.verbose = verbose
        self._print("Initializing INRIX_Downloader", 2)
        self.app_id = self._read_file(app_id_path)
        self.hash_token = self._read_file(hash_token_path)
        self.segments = self._read_segments(segments_path)
        self.token_path = token_path
        self.token = None
        self.token_expiry = None
        self._load_or_refresh_token()

    def _print(self, message, level):
        if self.verbose >= level:
            print(f"INRIX_Downloader: {message}")

    def _read_file(self, file_path):
        self._print(f"Reading file: {file_path}", 2)
        try:
            with open(file_path, 'r') as file:
                return file.read().strip()
        except Exception as e:
            self._print(f"Error reading file {file_path}: {e}", 1)
            raise

    def _read_segments(self, segments_path):
        self._print(f"Reading segments from: {segments_path}", 2)
        try:
            with open(segments_path, 'r') as file:
                content = file.read().strip()
                segments = content.split(',')
            self._print(f"Loaded {len(segments)} segments", 1)
            return segments
        except Exception as e:
            self._print(f"Error reading segments file {segments_path}: {e}", 1)
            raise

    def _load_or_refresh_token(self):
        self._print("Loading or refreshing token", 2)
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, 'r') as file:
                    token_data = json.load(file)
                    self.token = token_data['token']
                    self.token_expiry = datetime.fromisoformat(token_data['expiry'].replace('Z', '+00:00'))
                self._print("Token loaded from file", 2)

            if not self.token or self._token_needs_refresh():
                self._print("Token needs refresh, getting new token", 2)
                self._get_new_token()
        except Exception as e:
            self._print(f"Error loading token: {e}", 1)
            self._get_new_token()

    def _token_needs_refresh(self):
        if not self.token_expiry:
            return True
        now = datetime.now(timezone.utc)
        time_until_expiry = self.token_expiry - now
        self._print(f"Current time (UTC): {now}, Token expiry: {self.token_expiry}", 2)
        self._print(f"Time until token expiry: {time_until_expiry}", 2)
        
        # Check if token is already expired or will expire within an hour
        return time_until_expiry <= timedelta(0) or time_until_expiry < timedelta(hours=1)

    def _get_new_token(self):
        self._print("Getting new token", 2)
        try:
            url = f"https://uas-api.inrix.com/v1/appToken?appId={self.app_id}&hashToken={self.hash_token}"
            response = requests.get(url)
            
            response.raise_for_status()  # Raises an HTTPError for bad responses

            token_data = response.json()['result']
            self.token = token_data['token']
            self.token_expiry = datetime.fromisoformat(token_data['expiry'][:-2]).replace(tzinfo=timezone.utc)
            
            with open(self.token_path, 'w') as file:
                json.dump({
                    'token': self.token,
                    'expiry': self.token_expiry.isoformat()
                }, file)
            self._print("New token obtained and saved", 1)
        except Exception as e:
            self._print(f"Error getting new token: {e}", 1)
            raise

    def get_speed_data(self):
        self._print("Getting speed data", 1)
        base_url = "https://segment-api.inrix.com/v1/segments/speed"
        all_data = []

        try:
            for i in range(0, len(self.segments), 500):  # Changed from 1000 to 500
                segment_batch = self.segments[i:i+500]  # Changed from 1000 to 500

                self._print(f"Requesting data for segments {i+1} to {i+len(segment_batch)}", 2)

                params = {
                    'ids': ','.join(segment.strip() for segment in segment_batch),
                    'accesstoken': self.token
                    }

                response = requests.get(base_url, params=params)
                         
                response.raise_for_status()  # Raises an HTTPError for bad responses

                data = response.json()['result']['segmentspeeds'][0]['segments']
                all_data.extend(data)
                self._print(f"Received data for {len(data)} segments", 2)

            self._print(f"Total segments data received: {len(all_data)}", 1)
            return pd.DataFrame(all_data)
        except Exception as e:
            self._print(f"Error getting speed data: {e}", 1)
            raise


