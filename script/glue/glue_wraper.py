import sys
import time
import traceback
import boto3
import json
import os

# Adjust path to find your common scripts
# sys.path.insert(1, '/airflow/omniroute/')
try:
    from script.utility.utility import get_param

except ImportError:
    # Fallback for local testing if the script isn't found
    print("Warning: cdl_common_script not found. Using default params for testing.")
    def get_param(group, key): return 'us-east-1' # Default fallback

# ==========================================
# 1. INITIALIZATION & CONFIG LOADING
# ==========================================

# Fetch Environment Variables
env = get_param('GEN', 'env')
region_name = get_param('GEN', 'region')
# Handle cases where endpoint_url might not be needed or is None
endpoint_url = get_param('GEN', 'endpoint_url') 
role_arn = get_param('GEN', 'crawler_iam_role_arn')
env = "prod"
region = "us-east-1"
crawler_iam_role_arn = "arn:aws:iam::537124955775:role/bootcamp-poc-farhan"
endpoint_url = "https://glue.us-east-1.amazonaws.com"

if len(sys.argv) < 2:
    print("Error: Config file path argument is missing.")
    sys.exit(1)

config_file = sys.argv[1]
print(f"The config file is : {config_file}")

def read_config(json_file_path):
    with open(json_file_path, 'r') as f:
        data = f.read()
    # Dynamic replacement of {env} in the JSON
    return json.loads(data.replace("{env}", env))

# Load Config
config_data = read_config(config_file)
create_config = config_data.get("create_config", {})
jobrun_config = config_data.get("jobrun_config", {})
job_name = config_data.get("job_name")
create_glue_job = config_data.get("create_glue_job")

print(f"Target Glue Job Name: {job_name}")

# Initialize Glue Client
glue = boto3.client('glue', region_name=region_name)

# ==========================================
# 2. GLUE JOB MANAGEMENT FUNCTIONS
# ==========================================

def glue_get_job_status(job_name_target):
    """Checks if the Glue job already exists."""
    try:
        response = glue.get_job(JobName=job_name_target)
        return response['Job']['Name']
    except glue.exceptions.EntityNotFoundException:
        return "Job Not Found"
    except Exception:
        print("Unexpected Error while Getting Job Details: " + traceback.format_exc())
        raise

def glue_create_or_update_job(job_name_target, existing_job_status):
    """Creates the job if it doesn't exist, updates it if it does."""
    try:
        # If job doesn't exist, Create it
        if existing_job_status != job_name_target:
            if create_config:
                print(f"Creating new Glue Job: {job_name_target}")
                glue.create_job(Name=job_name_target, Role=role_arn, **create_config)
                print("Job Created Successfully")
            else:
                print("Skipping creation: 'create_config' not found in JSON.")

        # If job exists, Update it
        else:
            print(f"Job {job_name_target} exists. Updating configuration...")
            if create_config:
                # 'Role' is required for create_job but part of 'JobUpdate' dict for update_job
                # We reuse create_config but ensure Role is handled if needed by the API structure
                update_args = create_config.copy()
                update_args['Role'] = role_arn 
                
                glue.update_job(JobName=job_name_target, JobUpdate=update_args)
                print("Job Updated Successfully")
            else:
                print("Skipping update: 'create_config' not found in JSON.")

    except Exception:
        print("Unexpected Error while Creating/Updating Job: " + traceback.format_exc())
        raise

def glue_run_job(job_name_target):
    """Triggers the Glue Job."""
    try:
        print(f"Triggering Job: {job_name_target}")
        response = glue.start_job_run(JobName=job_name_target, **jobrun_config)
        print(f"Job Triggered. RunId: {response['JobRunId']}")
        return response
    except Exception:
        print("Unexpected Error while Running Job: " + traceback.format_exc())
        raise

def glue_check_state(job_name_target, run_id):
    """Polls the job status until completion."""
    print(f"Polling status for RunId: {run_id}...")
    try:
        while True:
            status = glue.get_job_run(JobName=job_name_target, RunId=run_id)
            run_state = status['JobRun']['JobRunState']
            
            if run_state == 'RUNNING':
                print(f"{job_name_target} - RUNNING...")
                time.sleep(30)

            elif run_state == 'STARTING':
                 print(f"{job_name_target} - STARTING...")
                 time.sleep(15)

            elif run_state == 'STOPPING':
                print(f"{job_name_target} - STOPPING...")
                time.sleep(10)

            elif run_state == 'STOPPED':
                print(f"{job_name_target} - STOPPED (User Cancelled/Forcefully Stopped)")
                raise Exception("Job stopped without completion")

            elif run_state == 'SUCCEEDED':
                print(f"{job_name_target} - SUCCEEDED")
                break
            
            elif run_state in ['FAILED', 'ERROR', 'TIMEOUT']:
                error_msg = status['JobRun'].get('ErrorMessage', 'No error message provided')
                print(f"{job_name_target} - {run_state}")
                print(f"Error Details: {error_msg}")
                raise Exception(f"Job Failed with state: {run_state}")
            
            else:
                # WAITING or other states
                print(f"{job_name_target} - {run_state}")
                time.sleep(10)

    except Exception:
        print("Unexpected Error while Checking State: " + traceback.format_exc())
        raise

# ==========================================
# 3. MAIN EXECUTION FLOW
# ==========================================
if __name__ == "__main__":
    
    # 1. Check / Create / Update Job
    if create_glue_job == 'True':
        current_status = glue_get_job_status(job_name)
        glue_create_or_update_job(job_name, current_status)

    # 2. Run Job
    run_details = glue_run_job(job_name)
    run_id = run_details['JobRunId']

    # 3. Monitor Job
    glue_check_state(job_name, run_id)
