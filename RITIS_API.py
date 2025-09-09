import requests
import duckdb
import time
from datetime import datetime, timedelta
import os
import uuid
import io
import zipfile
import warnings
import tempfile
import shutil
from typing import List, Literal

# Define valid types for parameters
SaveColumn = Literal["speed", "reference_speed", "travel_time_seconds"]
Column = Literal[
    "speed", 
    "historical_average_speed", 
    "reference_speed", 
    "travel_time_minutes",
    "confidence_score",
    "cvalue"
]
TimeUnit = Literal["minutes", "seconds"]
ConfidenceLevel = Literal[10, 20, 30]

# Type alias for a list of confidence levels in descending order
ConfidenceScoreList = List[ConfidenceLevel]

class RITIS_Downloader:
    def __init__(self,
                api_key: str,
                segments_path: str,
                download_path: str,
                last_run_path: str = 'last_run.txt',
                version: str = "v2",
                start_time: str = '00:00:00',
                end_time: str = '23:59:00', 
                bin_size: int = 15,
                units: TimeUnit = "minutes",
                columns: List[Column] = ["speed", "historical_average_speed", "reference_speed", "travel_time_minutes", "confidence_score", "cvalue"],
                confidence_score: ConfidenceScoreList = [30, 20, 10],
                verbose: int = 1,
                verify: bool = True,
                sleep_time: int = 60,
                daily_download_timeout_minutes: int = 300,
                save_columns: List[SaveColumn] = ["speed", "reference_speed", "travel_time_seconds"],
                max_date: str = None
                ):
        
        self.api_key = api_key
        self.version = version
        self.verbose = verbose
        self.save_columns = save_columns
        self.max_date = max_date
        self._print(f"Initializing RITIS_Downloader", 2)
        self.download_path = download_path
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)
        self.start_time = start_time
        self.end_time = end_time
        self.bin_size = bin_size
        self.units = units
        self.columns = columns
        self.confidence_score = confidence_score
        self.last_run = last_run_path
        self.verify = verify
        self.sleep_time = sleep_time
        self.daily_download_timeout_minutes = daily_download_timeout_minutes

        # supress warnings if verify is False
        if not self.verify:
            warnings.filterwarnings("ignore")
        
        # Get XD segments list
        try:
            with open(segments_path, 'r') as file:
                content = file.read().strip()
                # Support both comma-separated and line-separated formats
                if ',' in content:
                    self.xd_segments = [x.strip() for x in content.split(',')]
                else:
                    self.xd_segments = [x.strip() for x in content.split('\n') if x.strip()]
        except Exception as e:
            self._print(f"Failed to load XD segments, make sure path is correct: {segments_path}", 1)
            raise e
        
        self._print(f"Loaded {len(self.xd_segments)} XD segments", 1)

        # Set API URLs
        self.base_url = f"https://pda-api.ritis.org/{self.version}"
        self.submit_url = f"{self.base_url}/submit/export"
        self.status_url = f"{self.base_url}/jobs/status"
        self.results_url = f"{self.base_url}/results/export"

    # Helper function to print messages based on verbosity level
    def _print(self, message, level, same_line=False, new_line_first=False):
        if self.verbose >= level:
            if new_line_first:
                print()  # Print newline first
            if same_line and self.verbose == 1:
                print(f"\r{message}", end='', flush=True)
            else:
                print(message)
                

    def _submit_job(self, start_date, end_date, name, attempts=3):
        self._print(f"Submitting job: start_date={start_date}, end_date={end_date}, name={name}", 2)
        job_uuid = str(uuid.uuid4())
        data = {
            "uuid": job_uuid,
            "segments": {
                "type": "xd",
                "ids": self.xd_segments
            },
            "dates": [{
                "start": start_date,
                "end": end_date
            }],
            "times": [{
                "start": self.start_time,
                "end": self.end_time
            }],
            "dow": [0, 1, 2, 3, 4, 5, 6],
            "dsFields": [{
                "id": "inrix_xd",
                "columns": self.columns,
                "qualityFilter": {
                    "thresholds": self.confidence_score
                }
            }],
            "granularity": {
                "type": "minutes",
                "value": self.bin_size
            },
            "travelTimeUnits": self.units,
            "includeIsoTzd": False
        }
        
        self._print(f"Submitting job with UUID: {job_uuid}", 2)
        # Print the request enpoint and headers for debugging
        self._print(f"Request Endpoint:\n{self.submit_url}?key={self.api_key}", 2)
        self._print(f"Request Data:\n{data}", 2)

        # Try to submit the job up to n times
        sleep_time = 0
        for i in range(attempts):
            time.sleep(sleep_time)
            response = requests.post(f"{self.submit_url}?key={self.api_key}", json=data, verify=self.verify)
            if response.status_code == 200 or i == attempts-1:
                break
            else:
                sleep_time = 10 * ((i+1)**2)
                self._print(f"Job submission attempt {i+1}/{attempts} failed, trying again in {sleep_time} seconds", 1)

        self._print(f"Job submission response: {response.status_code}", 2)
        if response.status_code == 200:
            job_id = response.json()['id']
            self._print(f"Job submitted successfully. Job ID: {job_id}", 1)
            return job_id, job_uuid
        else:
            self._print(f"Job submission failed: {response.text}", 1)
            raise Exception(f"Job submission failed: {response.text}")

    def _check_job_status(self, job_id, start_time=None):
        response = requests.get(f"{self.status_url}?key={self.api_key}&jobId={job_id}", verify=self.verify)
        if response.status_code == 200:
            status = response.json()
            current_time = datetime.now()
            time_str = current_time.strftime("%H:%M:%S")
            
            if start_time:
                elapsed_seconds = int((current_time - start_time).total_seconds())
                self._print(f"Job Progress Last Update at: {time_str} | {status['progress']}% complete |  {elapsed_seconds}s elapsed", 1, same_line=True)
            else:
                self._print(f"Job Progress: {status['progress']}% | {time_str}", 1, same_line=True)
            
            return status['state']
        elif response.status_code == 429:
            self._print(f"Rate limit exceeded with message:\n {response.text}", 1)
            return 'RATE_LIMITED'
        else:
            self._print(f"Failed to get job status: {response.text}", 1)
            raise Exception(f"Failed to get job status: {response.text}")

    def _download_and_process_job_results(self, uuid, job_name):
        self._print(f"Downloading and processing results for UUID: {uuid}", 2)
        response = requests.get(f"{self.results_url}?key={self.api_key}&uuid={uuid}", stream=True, verify=self.verify)
        if response.status_code == 200:
            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as temp_file:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                    with zip_ref.open('Readings.csv') as csv_file:
                        # Use shutil to efficiently copy the file contents
                        shutil.copyfileobj(csv_file, temp_file) 
                temp_file_path = temp_file.name
            try:
                # Process the CSV data and save as Parquet using DuckDB
                parquet_filename = os.path.join(self.download_path, f"{job_name}.parquet")
                
                # Build dynamic SQL based on selected columns
                select_columns = ["xd_id", "measurement_tstamp"]
                select_columns.extend([f"{col}::FLOAT as {col}" for col in self.save_columns])
                select_sql = ",\n                            ".join(select_columns)
                
                duckdb.sql(f"""
                    COPY (
                        SELECT 
                            {select_sql}
                        FROM '{temp_file_path}'
                    ) TO '{parquet_filename}' (FORMAT 'parquet')
                """)
                self._print(f"Saved parquet file: {parquet_filename}", 1, new_line_first=True)
                return True
            finally:
                # Ensure temporary file is always removed
                os.unlink(temp_file_path)
        else:
            self._print(f"Failed to download results: {response.text}", 1)
            return None

    def _get_dates(self):
        self._print("Getting dates for daily download", 2)
        try:
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            date_list = []
            
            # Parse max_date if provided
            max_date_obj = None
            if self.max_date:
                try:
                    max_date_obj = datetime.strptime(self.max_date, '%Y-%m-%d').date()
                    self._print(f"Max date set to: {max_date_obj}", 2)
                except ValueError:
                    raise ValueError(f"Invalid max_date format. Expected YYYY-MM-DD, got: {self.max_date}")
            
            with open(self.last_run, 'r') as f:
                last_run_str = f.read().strip()  # Remove any leading/trailing whitespace
            
            # Try parsing with different formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']:
                try:
                    last_run = datetime.strptime(last_run_str, fmt).date()
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(f"Unable to parse date: {last_run_str}")
            
            # Determine the end date (either yesterday or max_date, whichever is earlier)
            end_date = yesterday
            if max_date_obj and max_date_obj < yesterday:
                end_date = max_date_obj
                self._print(f"Using max_date as end date: {end_date}", 2)
            
            while last_run < end_date:
                last_run += timedelta(days=1)
                date_list.append(last_run.strftime("%Y-%m-%d"))
            
            self._print(f"Dates to process: {date_list}", 2)
            return date_list
        except Exception as e:
            raise Exception(f"Failed to get dates: {e}")

    def daily_download(self):
        self._print("Starting daily download", 1)
        date_list = self._get_dates()
        if not date_list:
            self._print("Data is already updated through yesterday, or something went wrong.", 1)
            return

        # Iterate through each date
        for date in date_list:
            job_name = str(date)
            # Use the same date for both start and end, but add one day to the end date
            start_date = date
            end_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            # Failed jobs will be retried once using this counter
            failed_attempts = 0

            job_id, job_uuid = self._submit_job(start_date, end_date, job_name)

            if job_id:
                start_time = datetime.now()
                max_time = timedelta(minutes=self.daily_download_timeout_minutes)
                
                while datetime.now() - start_time < max_time:
                    status = self._check_job_status(job_id, start_time)
                    if status == 'SUCCEEDED':
                        if self._download_and_process_job_results(job_uuid, job_name):
                            # Update last run date after each successful download
                            with open(self.last_run, 'w') as f:
                                f.write(f"{date} 00:00:00")
                        break
                    elif status in ['KILLED', 'FAILED']:
                        failed_attempts += 1
                        if failed_attempts <= 1:
                            self._print(f"Job {job_id} failed with state: {status}, retrying now", 1, new_line_first=True)
                            job_id, job_uuid = self._submit_job(start_date, end_date, job_name)
                        else:
                            raise Exception(f"Job {job_id} failed with state: {status['state']}")
                    elif status == 'RATE_LIMITED':
                        self._print(f"Rate limit exceeded, mandatory nap time for 5 minutes...", 1, new_line_first=True)
                        time.sleep(300)
                    time.sleep(self.sleep_time)
                else:
                    raise Exception(f"Job {job_id} timed out after {self.daily_download_timeout_minutes} minutes")

        self._print("Daily download completed", 1, new_line_first=True)


    def single_download(self, start_date, end_date, job_name):
        self._print(f"Starting single download: start_date={start_date}, end_date={end_date}, job_name={job_name}", 1)
        job_name = job_name.replace(' ', '_').replace(':', '')
        job_id, job_uuid = self._submit_job(start_date, end_date, job_name)
        if job_id:
            start_time = datetime.now()
            while True:
                status = self._check_job_status(job_id, start_time)
                if status == 'SUCCEEDED':
                    self._download_and_process_job_results(job_uuid, job_name)
                    break
                elif status in ['KILLED', 'FAILED']:
                    self._print(f"Job {job_id} failed with state: {status}", 1, new_line_first=True)
                    break
                time.sleep(self.sleep_time)
        self._print("Single download completed", 1, new_line_first=True)
