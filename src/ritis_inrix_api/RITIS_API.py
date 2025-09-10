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
from typing import List, Literal, Union

# Define valid types for parameters
Column = Literal[
    "speed",
    "historical_average_speed",
    "reference_speed",
    "travel_time",
    "confidence_score",
    "confidence",
    "data_quality",
    "cvalue"
]
TimeUnit = Literal["minutes", "seconds"]
ConfidenceLevel = Literal[10, 20, 30]

# Type alias for a list of confidence levels in descending order
ConfidenceScoreList = List[ConfidenceLevel]

class RITIS_Downloader:
    """
    A class to download data from the RITIS API.

    This class handles submitting data export jobs, checking their status,
    and downloading the results. It can be used for both daily incremental
    downloads and ad-hoc single downloads.
    """
    def __init__(self,
                api_key: str,
                segments: Union[str, List[Union[str, int]]],
                download_path: str = None,
                last_run_path: str = 'last_run.txt',
                version: str = "v2",
                start_time: str = '00:00:00',
                end_time: str = '23:59:00', 
                bin_size: int = 15,
                units: TimeUnit = "minutes",
                columns: List[Column] = Column.__args__,
                confidence_score: ConfidenceScoreList = [30, 20, 10],
                verbose: int = 1,
                verify: bool = True,
                sleep_time: int = 60,
                daily_download_timeout_minutes: int = 300,
                max_date: str = None
                ):
        """
        Initializes the RITIS_Downloader.

        Args:
            api_key (str): Your RITIS API key.
            segments (Union[str, List[Union[str, int]]]): A list of XD segment IDs or a
                file path to a text file containing segment IDs (one per line or comma-separated).
            download_path (str, optional): The default path to save downloaded files.
                Defaults to None.
            last_run_path (str, optional): The path to the file that stores the timestamp
                of the last successful run for daily downloads. Defaults to 'last_run.txt'.
            version (str, optional): The RITIS API version to use. Defaults to "v2".
            start_time (str, optional): The start time for data retrieval in 'HH:MM:SS' format.
                Defaults to '00:00:00'.
            end_time (str, optional): The end time for data retrieval in 'HH:MM:SS' format.
                Defaults to '23:59:00'.
            bin_size (int, optional): The data aggregation interval in minutes. Defaults to 15.
            units (TimeUnit, optional): The units for travel time ('minutes' or 'seconds').
                Defaults to "minutes".
            columns (List[Column], optional): A list of data columns to download.
                Defaults to all available columns.
            confidence_score (ConfidenceScoreList, optional): A list of confidence score
                thresholds to apply, in descending order. Defaults to [30, 20, 10].
            verbose (int, optional): The verbosity level for printing messages.
                0 = silent, 1 = standard, 2 = debug. Defaults to 1.
            verify (bool, optional): Whether to verify SSL certificates for API requests.
                Defaults to True.
            sleep_time (int, optional): The time in seconds to wait between checking job status.
                Defaults to 60.
            daily_download_timeout_minutes (int, optional): The timeout in minutes for a single
                day's download job. Defaults to 300.
            max_date (str, optional): The maximum date to download data for in 'YYYY-MM-DD' format.
                Used as a cutoff for daily downloads. Defaults to None.
        """
        
        # Validate api_key is not empty
        if not api_key or not isinstance(api_key, str):
            raise ValueError("api_key must be a non-empty string")
            
        # Validate verbose level
        if not isinstance(verbose, int) or verbose < 0 or verbose > 2:
            raise ValueError("verbose must be 0, 1, or 2")
            
        # Validate max_date format if provided
        if max_date:
            try:
                datetime.strptime(max_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"max_date must be in 'YYYY-MM-DD' format. Got: {max_date}")
                
        self.api_key = api_key
        self.version = version
        self.verbose = verbose
        self.max_date = max_date
        self._print(f"Initializing RITIS_Downloader", 2)
        self.download_path = download_path
        if self.download_path and not os.path.exists(self.download_path):
            os.makedirs(self.download_path)
        # Validate times format
        for time_str in [start_time, end_time]:
            try:
                datetime.strptime(time_str, '%H:%M:%S')
            except ValueError:
                raise ValueError(f"Time must be in 'HH:MM:SS' format. Got: {time_str}")
        
        # Validate bin_size (must be positive integer)
        if not isinstance(bin_size, int) or bin_size <= 0:
            raise ValueError(f"bin_size must be a positive integer. Got: {bin_size}")
            
        # Validate units
        valid_units = list(TimeUnit.__args__)
        if units not in valid_units:
            raise ValueError(f"Invalid units: {units}. Valid values are: {valid_units}")
            
        self.start_time = start_time
        self.end_time = end_time
        self.bin_size = bin_size
        self.units = units
        
        # Validate columns
        valid_columns = list(Column.__args__)
        invalid_columns = [col for col in columns if col not in valid_columns]
        if invalid_columns:
            raise ValueError(f"Invalid column(s): {invalid_columns}. Valid columns are: {valid_columns}")
        self.columns = columns
        
        # Validate confidence scores
        if not all(score in ConfidenceLevel.__args__ for score in confidence_score):
            raise ValueError(f"Invalid confidence score(s). Valid values are: {list(ConfidenceLevel.__args__)}")
        self.confidence_score = confidence_score
        
        # Validate last_run_path
        if not isinstance(last_run_path, str):
            raise ValueError("last_run_path must be a string")
            
        # Validate sleep_time (must be positive integer)
        if not isinstance(sleep_time, int) or sleep_time <= 0:
            raise ValueError(f"sleep_time must be a positive integer. Got: {sleep_time}")
            
        # Validate daily_download_timeout_minutes (must be positive integer)
        if not isinstance(daily_download_timeout_minutes, int) or daily_download_timeout_minutes <= 0:
            raise ValueError(f"daily_download_timeout_minutes must be a positive integer. Got: {daily_download_timeout_minutes}")
            
        self.last_run = last_run_path
        self.verify = verify
        self.sleep_time = sleep_time
        self.daily_download_timeout_minutes = daily_download_timeout_minutes

        # supress warnings if verify is False
        if not self.verify:
            warnings.filterwarnings("ignore")
        
        # Get XD segments list
        if isinstance(segments, list):
            self.xd_segments = [str(x) for x in segments]
        elif isinstance(segments, str):
            try:
                with open(segments, 'r') as file:
                    content = file.read().strip()
                    # Support both comma-separated and line-separated formats
                    if ',' in content:
                        self.xd_segments = [x.strip() for x in content.split(',')]
                    else:
                        self.xd_segments = [x.strip() for x in content.split('\n') if x.strip()]
            except Exception as e:
                self._print(f"Failed to load XD segments from path, make sure path is correct: {segments}", 1)
                raise e
        else:
            raise TypeError("segments must be a list of segment IDs or a file path string.")
        
        self._print(f"Loaded {len(self.xd_segments)} XD segments", 1)

        # Set API URLs
        self.base_url = f"https://pda-api.ritis.org/{self.version}"
        self.submit_url = f"{self.base_url}/submit/export"
        self.status_url = f"{self.base_url}/jobs/status"
        self.results_url = f"{self.base_url}/results/export"

    # Helper function to print messages based on verbosity level
    def _print(self, message, level, same_line=False, new_line_first=False):
        """
        Helper function to print messages based on the verbosity level.

        Args:
            message (str): The message to print.
            level (int): The verbosity level required to print the message.
            same_line (bool, optional): If True, overwrite the current line. Defaults to False.
            new_line_first (bool, optional): If True, print a newline before the message. Defaults to False.
        """
        if self.verbose >= level:
            if new_line_first:
                print()  # Print newline first
            if same_line and self.verbose == 1:
                print(f"\r{message}", end='', flush=True)
            else:
                print(message)
                

    def _submit_job(self, start_date, end_date, name, attempts=3):
        """
        Submits a data export job to the RITIS API.

        Args:
            start_date (str): The start date for the job in 'YYYY-MM-DD' format.
            end_date (str): The end date for the job in 'YYYY-MM-DD' format.
            name (str): A name for the job.
            attempts (int, optional): The number of times to attempt job submission. Defaults to 3.

        Returns:
            tuple[str, str]: A tuple containing the job ID and the job UUID.

        Raises:
            Exception: If the job submission fails after all attempts.
        """
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
        self._print(f"Request Endpoint:\n{self.submit_url}?key=KEY_NOT_SHOWN_FOR_SECURITY", 2)
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
        """
        Checks the status of a submitted job.

        Args:
            job_id (str): The ID of the job to check.
            start_time (datetime, optional): The time the job started, used for calculating
                elapsed time. Defaults to None.

        Returns:
            str: The state of the job (e.g., 'SUCCEEDED', 'KILLED', 'FAILED', 'RATE_LIMITED').

        Raises:
            Exception: If the status check request fails.
        """
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

    def _download_and_process_job_results(self, uuid, job_name, download_path=None):
        """
        Downloads and processes the results of a completed job.

        The results are downloaded as a zip file, extracted, and then processed.
        If a `download_path` is provided, the data is saved as a Parquet file.
        Otherwise, it's returned as a Pandas DataFrame.

        Args:
            uuid (str): The UUID of the job to download.
            job_name (str): The name of the job, used for the output filename.
            download_path (str, optional): The path to save the Parquet file.
                If None, a DataFrame is returned. Defaults to None.

        Returns:
            Union[bool, pd.DataFrame]: True if the file is saved successfully, or a
            Pandas DataFrame if `download_path` is None.

        Raises:
            Exception: If the result download fails.
        """
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
                # Build dynamic SQL based on selected columns
                select_columns = ["xd_id", "measurement_tstamp"]
                for col in self.columns:
                    if col == "travel_time":
                        select_columns.append(f'"travel_time_{self.units}"::FLOAT as "travel_time_{self.units}"')
                    else:
                        select_columns.append(f'"{col}"::FLOAT as "{col}"')
                select_sql = ",\n".join(select_columns)
                
                sql_query = f"""
                    SELECT 
                        {select_sql}
                    FROM '{temp_file_path}'
                """
                self._print(f"SQL query:\n{sql_query}", 2)
                # If download_path is not provided, return a DataFrame
                if not download_path:
                    self._print(f"Returning DataFrame for job: {job_name}", 1, new_line_first=True)
                    return duckdb.sql(sql_query).df()

                # Process the CSV data and save as Parquet using DuckDB
                parquet_filename = os.path.join(download_path, f"{job_name}.parquet")
                
                duckdb.sql(f"""
                    COPY ({sql_query}) TO '{parquet_filename}' (FORMAT 'parquet')
                """)
                self._print(f"Saved parquet file: {parquet_filename}", 1, new_line_first=True)
                return True
            finally:
                # Ensure temporary file is always removed
                os.unlink(temp_file_path)
        else:
            self._print(f"Failed to download results: {response.text}", 1)
            raise Exception(f"Failed to download results: {response.text}")

    def _get_dates(self):
        """
        Calculates the list of dates to download for the daily_download method.

        It reads the last run date, and generates a list of dates from then until
        yesterday or the `max_date`.

        Returns:
            List[str]: A list of dates in 'YYYY-MM-DD' format.

        Raises:
            Exception: If it fails to read or parse the last run date file.
            ValueError: If `max_date` has an invalid format.
        """
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
        """
        Performs a daily download of RITIS data, starting from the last run date.

        This method checks for the last run date from the file specified by `last_run_path`.
        It then downloads data for each day from the last run date up to yesterday
        (or `max_date` if specified and earlier than yesterday).

        The downloaded data is saved as Parquet files in the `download_path` provided
        during initialization. The `last_run_path` file is updated with the date of the
        last successfully downloaded day.

        Raises:
            ValueError: If `download_path` was not provided during initialization.
            Exception: If a job fails, times out, or if there's an issue getting dates.
        """
        if not self.download_path:
            raise ValueError("A 'download_path' must be provided during initialization to use daily_download.")
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
                        if self._download_and_process_job_results(job_uuid, job_name, self.download_path):
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


    def single_download(self, start_date, end_date, job_name, download_path=None):
        """
        Performs a single, ad-hoc download for a specified date range.

        Args:
            start_date (str): The start date for the download in 'YYYY-MM-DD' format.
            end_date (str): The end date for the download in 'YYYY-MM-DD' format.
            job_name (str): A name for the job, used for the output filename.
            download_path (str, optional): The path to save the downloaded file.
                If not provided, the `download_path` from initialization is used.
                If neither is provided, the data is returned as a DataFrame.
                Defaults to None.

        Returns:
            Union[bool, pd.DataFrame, None]: If a `download_path` is used, returns True
            on success. If no `download_path` is specified, returns a Pandas DataFrame.
            Returns None if the job does not complete successfully.

        Raises:
            Exception: If the job fails or does not complete.
        """
        effective_path = download_path or self.download_path
        self._print(f"Starting single download: start_date={start_date}, end_date={end_date}, job_name={job_name}", 1)
        job_name = job_name.replace(' ', '_').replace(':', '')
        job_id, job_uuid = self._submit_job(start_date, end_date, job_name)
        if job_id:
            start_time = datetime.now()
            while True:
                status = self._check_job_status(job_id, start_time)
                if status == 'SUCCEEDED':
                    result = self._download_and_process_job_results(job_uuid, job_name, effective_path)
                    self._print("Single download completed", 1, new_line_first=True)
                    return result
                elif status in ['KILLED', 'FAILED']:
                    self._print(f"Job {job_id} failed with state: {status}", 1, new_line_first=True)
                    raise Exception(f"Job {job_id} failed with state: {status}")
                time.sleep(self.sleep_time)
        self._print("Single download did not complete successfully", 1, new_line_first=True)
        raise Exception("Single download did not complete successfully")