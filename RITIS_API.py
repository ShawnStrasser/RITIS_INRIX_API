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

class RITIS_Downloader:
    def __init__(self,
                api_key,
                segments_path,
                download_path,
                last_run_path='last_run.txt',
                version="v2",
                start_time='00:00:00',
                end_time='23:59:00', 
                bin_size=15,
                units="minutes",
                columns=["speed", "historical_average_speed", "reference_speed", "travel_time_minutes", "confidence_score", "cvalue"],
                confidence_score=[30, 20, 10],
                verbose=1,
                verify=True,
                sleep_time=60,
                daily_download_timeout_minutes=60):  
        
        self.api_key = api_key
        self.version = version
        self.verbose = verbose
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
                self.xd_segments = [x.strip() for x in file.read().split(',')]
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
    def _print(self, message, level):
        if self.verbose >= level:
            print(message)


    def _submit_job(self, start_date, end_date, name):
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
        response = requests.post(f"{self.submit_url}?key={self.api_key}", json=data, verify=self.verify)
        self._print(f"Job submission response: {response.status_code}", 2)
        if response.status_code == 200:
            job_id = response.json()['id']
            self._print(f"Job submitted successfully. Job ID: {job_id}", 1)
            return job_id, job_uuid
        else:
            self._print(f"Job submission failed: {response.text}", 1)
            raise Exception(f"Job submission failed: {response.text}")

    def _check_job_status(self, job_id):
        response = requests.get(f"{self.status_url}?key={self.api_key}&jobId={job_id}", verify=self.verify)
        if response.status_code == 200:
            status = response.json()
            self._print(f"Job Progress: {status['progress']}%", 2)
            return status
        else:
            self._print(f"Failed to get job status: {response.text}", 1)
            return None

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
                duckdb.sql(f"COPY (SELECT * FROM '{temp_file_path}') TO '{parquet_filename}' (FORMAT 'parquet')")
                self._print(f"Saved parquet file: {parquet_filename}", 1)
                return True
            finally:
                # Ensure temporary file is always removed
                os.unlink(temp_file_path)
        else:
            self._print(f"Failed to download results: {response.text}", 1)
            return None

    def _get_dates(self):
        self._print("Getting dates for daily download", 2)
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        date_list = []
        
        with open(self.last_run, 'r') as f:
            last_run = datetime.strptime(f.read(), '%Y-%m-%d %H:%M:%S').date()
        
        while last_run <= yesterday:
            date_list.append(last_run.strftime("%Y-%m-%d"))
            last_run += timedelta(days=1)
        
        self._print(f"Dates to process: {date_list}", 2)
        return date_list

    def daily_download(self):
        self._print("Starting daily download", 1)
        try:
            date_list = self._get_dates()
            if not date_list:
                self._print("Data is already updated through yesterday, or something went wrong.", 1)
                return

            for date in date_list:
                job_name = str(date)
                # Use the same date for both start and end, but add one day to the end date
                start_date = date
                end_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                job_id, job_uuid = self._submit_job(start_date, end_date, job_name)
                if job_id:
                    start_time = datetime.now()
                    max_time = timedelta(minutes=self.daily_download_timeout_minutes)
                    
                    while datetime.now() - start_time < max_time:
                        status = self._check_job_status(job_id)
                        if status['state'] == 'SUCCEEDED':
                            if self._download_and_process_job_results(job_uuid, job_name):
                                # Update last run date after each successful download
                                with open(self.last_run, 'w') as f:
                                    f.write(f"{date} 00:00:00")
                            break
                        elif status['state'] in ['KILLED', 'FAILED']:
                            raise Exception(f"Job {job_id} failed with state: {status['state']}")
                        time.sleep(self.sleep_time)
                    else:
                        raise Exception(f"Job {job_id} timed out after {self.daily_download_timeout_minutes} minutes")

            self._print("Daily download completed", 1)
        except Exception as e:
            self._print(f"An error occurred during daily download: {str(e)}", 1)
            raise e

    def single_download(self, start_date, end_date, job_name):
        self._print(f"Starting single download: start_date={start_date}, end_date={end_date}, job_name={job_name}", 1)
        job_name = job_name.replace(' ', '_').replace(':', '')
        job_id, job_uuid = self._submit_job(start_date, end_date, job_name)
        if job_id:
            while True:
                status = self._check_job_status(job_id)
                if status['state'] == 'SUCCEEDED':
                    self._download_and_process_job_results(job_uuid, job_name)
                    break
                elif status['state'] in ['KILLED', 'FAILED']:
                    self._print(f"Job {job_id} failed with state: {status['state']}", 1)
                    break
                time.sleep(self.sleep_time)
        self._print("Single download completed", 1)