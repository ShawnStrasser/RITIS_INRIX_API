import requests
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
import os
import tempfile
from typing import List, Union

class INRIX_Downloader:
    """
    A class to download data from the INRIX API.

    This class handles authentication with the INRIX API using app_id and hash_token,
    and retrieves speed data for specified XD segments. It manages token refresh
    automatically and supports batch processing of segment requests.

    Args:
        app_id (str): Your INRIX application ID.
        hash_token (str): Your INRIX hash token for authentication.
        segments (Union[str, List[Union[str, int]]]): A list of XD segment IDs or a
            file path to a text file containing segment IDs (comma-separated).
        verbose (int, optional): The verbosity level for printing messages.
            0 = silent, 1 = standard, 2 = debug. Defaults to 1.
    
    Raises:
        ValueError: If app_id or hash_token is empty, or if segments is invalid.
        TypeError: If segments is neither a list nor a string path.
    """
    def __init__(self, app_id: str, hash_token: str, segments: Union[str, List[Union[str, int]]], verbose: int = 1):
        # Validate inputs
        if not app_id or not isinstance(app_id, str):
            raise ValueError("app_id must be a non-empty string")
        if not hash_token or not isinstance(hash_token, str):
            raise ValueError("hash_token must be a non-empty string")
        if not isinstance(verbose, int) or verbose < 0 or verbose > 2:
            raise ValueError("verbose must be 0, 1, or 2")
            
        self.verbose = verbose
        self._print("Initializing INRIX_Downloader", 2)
        self.app_id = app_id
        self.hash_token = hash_token
        
        # Process segments input
        if isinstance(segments, list):
            self.segments = [str(x) for x in segments]
            self._print(f"Using provided list of {len(self.segments)} segments", 2)
        elif isinstance(segments, str):
            self.segments = self._read_segments(segments)
        else:
            raise TypeError("segments must be a list of segment IDs or a file path string")
            
        self.token = None
        self.token_expiry = None
        self.token_path = os.path.join(tempfile.gettempdir(), f"inrix_token_{self.app_id}.json")
        self._load_token_from_file()

    def _print(self, message: str, level: int) -> None:
        """
        Helper function to print messages based on verbosity level.

        Args:
            message (str): The message to print.
            level (int): The verbosity level required to print the message.
                0 = silent, 1 = standard, 2 = debug.
        """
        if self.verbose >= level:
            print(f"INRIX_Downloader: {message}")

    def _read_file(self, file_path: str) -> str:
        """
        Reads and returns the contents of a file.

        Args:
            file_path (str): Path to the file to read.

        Returns:
            str: The contents of the file with leading/trailing whitespace removed.

        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If there's an error reading the file.
        """
        self._print(f"Reading file: {file_path}", 2)
        try:
            with open(file_path, 'r') as file:
                return file.read().strip()
        except Exception as e:
            self._print(f"Error reading file {file_path}: {e}", 1)
            raise

    def _read_segments(self, segments_path: str) -> List[str]:
        """
        Reads XD segment IDs from a file.

        The file should contain segment IDs either one per line or comma-separated.
        Leading/trailing whitespace is stripped from each segment ID.

        Args:
            segments_path (str): Path to the file containing segment IDs.

        Returns:
            List[str]: A list of segment IDs as strings.

        Raises:
            FileNotFoundError: If the segments file does not exist.
            IOError: If there's an error reading the file.
            ValueError: If no valid segments are found in the file.
        """
        self._print(f"Reading segments from: {segments_path}", 2)
        try:
            with open(segments_path, 'r') as file:
                content = file.read().strip()
                # Try comma-separated format first
                segments = [seg.strip() for seg in content.replace('\n', ',').split(',')]
                # Filter out empty strings
                segments = [seg for seg in segments if seg]
                
                if not segments:
                    raise ValueError(f"No valid segments found in file: {segments_path}")
                    
                self._print(f"Loaded {len(segments)} segments", 1)
                return segments
                
        except FileNotFoundError:
            self._print(f"Segments file not found: {segments_path}", 1)
            raise
        except Exception as e:
            self._print(f"Error reading segments file {segments_path}: {e}", 1)
            raise

    def _load_token_from_file(self):
        self._print(f"Attempting to load token from {self.token_path}", 2)
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, 'r') as f:
                    token_data = json.load(f)
                    self.token = token_data['token']
                    self.token_expiry = datetime.fromisoformat(token_data['expiry']).replace(tzinfo=timezone.utc)
                    self._print("Token loaded from file.", 1)
        except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
            self._print(f"Could not load token from file: {e}", 1)
            self.token = None
            self.token_expiry = None

    def _save_token_to_file(self):
        self._print(f"Saving token to {self.token_path}", 2)
        token_data = {
            'token': self.token,
            'expiry': self.token_expiry.isoformat()
        }
        with open(self.token_path, 'w') as f:
            json.dump(token_data, f)

    def _token_needs_refresh(self) -> bool:
        """
        Checks if the current API token needs to be refreshed.

        A token needs refresh if it's missing, expired, or will expire within an hour.

        Returns:
            bool: True if the token needs to be refreshed, False otherwise.
        """
        if not self.token_expiry:
            return True
        now = datetime.now(timezone.utc)
        time_until_expiry = self.token_expiry - now
        self._print(f"Current time (UTC): {now}, Token expiry: {self.token_expiry}", 2)
        self._print(f"Time until token expiry: {time_until_expiry}", 2)
        
        # Check if token is already expired or will expire within an hour
        return time_until_expiry <= timedelta(0) or time_until_expiry < timedelta(hours=1)

    def _get_new_token(self) -> None:
        """
        Obtains a new authentication token from the INRIX API.

        Makes an API request to get a new token using the app_id and hash_token.
        Updates the token and its expiry time, and saves them to a temporary file.

        Raises:
            requests.HTTPError: If the API request fails.
            Exception: For other errors during token retrieval or processing.
        """
        self._print("Getting new token", 2)
        try:
            url = f"https://uas-api.inrix.com/v1/appToken?appId={self.app_id}&hashToken={self.hash_token}"
            response = requests.get(url)
            
            response.raise_for_status()  # Raises an HTTPError for bad responses

            token_data = response.json()['result']
            self.token = token_data['token']
            self.token_expiry = datetime.fromisoformat(token_data['expiry'][:-2]).replace(tzinfo=timezone.utc)
            self._save_token_to_file()
            
            self._print("New token obtained and saved", 1)
        except Exception as e:
            self._print(f"Error getting new token: {e}", 1)
            raise

    def get_speed_data(self) -> pd.DataFrame:
        """
        Retrieves current speed data for all configured segments from INRIX API.

        The method automatically handles token refresh if needed and processes segments
        in batches of 500 to comply with API limits. Each API response includes speed
        data for the requested XD segments.

        Returns:
            pd.DataFrame: A DataFrame containing the speed data for all segments.
                Columns typically include segment ID, speed, reference speed,
                confidence score, and timestamp.

        Raises:
            requests.HTTPError: If the API request fails.
            Exception: For other errors during data retrieval or processing.
        """
        self._print("Getting speed data", 1)
        
        if not self.token or self._token_needs_refresh():
            self._get_new_token()
            
        base_url = "https://segment-api.inrix.com/v1/segments/speed"
        all_data = []

        try:
            for i in range(0, len(self.segments), 500):  # Process in batches of 500
                segment_batch = self.segments[i:i+500]

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



