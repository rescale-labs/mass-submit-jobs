MSJ (mass-job-submit) is a tool for submitting large numbers of jobs programmatically to the Rescale platform.

Requirements:

* Environment variable
RESCALE_API_KEY — Rescale user token must be present in the shell before running the script.

* Local tools
      - Python 3.8+ with the requests package (only external library).
      - rescale-cli executable on the system (in the PATH) for file uploads.

* Input CSVs in the working directory
      - job_inputs_matrix.csv — one row per job with the eight required columns.
      - msj_config.csv — key/value file holding throttle, retry, and logging settings.

* Actual job input files
All local files referenced in the input_files column in  job_inputs_matrix.csv must exist and be accessible to the script.

* Optional helper
csv_synth.py (included) can generate large synthetic versions of job_inputs_matrix.csv

MSJ flow:

* Reads configuration and job definitions
The script loads msj_config.csv for throttle and retry settings, and parses job_inputs_matrix.csv to pull job names, file lists, hardware settings, and multi-analysis details.

* Uploads all required input files
For each row it uses rescale-cli upload to transfer every listed file, capturing the returned Rescale fileId values.

* Builds job-submission JSON on the fly
It creates a complete job payload for each row—duplicating the file list and hardware block in every analysis—then sends it to the Rescale API.

* Launches the jobs and logs results
After creating the draft job it immediately calls the /submit/ endpoint to start it, records the job ID, time, and retries in msj_ledger.csv, and writes detailed messages to msj.log.

* Manages concurrency and pacing
A thread pool handles uploads and API calls in parallel, while a configurable burst/-gap scheduler spaces out submissions to avoid overloading the platform.
