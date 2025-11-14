import json
import requests
import base64
from typing import Tuple, Dict, Any
import pandas as pd
from dotenv import load_dotenv
import os
from io import BytesIO
import warnings
import time
from datetime import datetime

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

def create_auth_header(api_key: str, api_secret: str) -> str:
    """Create the Basic Auth header from API key and secret."""
    credentials = f"{api_key}:{api_secret}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"

def set_up_environment():
    load_dotenv()
        
    start_date = "01/01/2022"
    end_date = pd.Timestamp.now().strftime('%Y-%m-%d')
    
    # Get configuration from environment variables
    firm_domain = os.getenv('ADDEPAR_FIRM_DOMAIN')
    firm_id = os.getenv('ADDEPAR_FIRM_ID')
    api_key = os.getenv('ADDEPAR_API_KEY')
    api_secret = os.getenv('ADDEPAR_API_SECRET')
        
    base_url = f"https://{firm_domain}"
    
    return api_key, api_secret, firm_id, base_url, start_date, end_date
    
def fetch_addepar_data(view_type: str, view_id: str, api_key: str, api_secret: str, firm_id: str, base_url: str, 
                      start_date: str, end_date: str) -> Tuple[pd.DataFrame, bool, str]:
    """Fetch data from Addepar API."""
    headers = {
        'Authorization': create_auth_header(api_key, api_secret),
        'Addepar-Firm': firm_id,
        'Accept': 'application/vnd.api+json'
    }
    
    url = f"{base_url}/api/v1/{view_type}/views/{view_id}/results"
    params = {
        'portfolio_type': 'firm',
        'format': 'json',
        'portfolio_id': 1,
        'output_type': 'xlsx',
        'start_date': start_date,
        'end_date': end_date
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            # Use BytesIO to read the XLSX data from the response content
            xlsx_data = BytesIO(response.content)
            try:
                data = pd.read_excel(xlsx_data, engine="openpyxl")
                if data.empty:
                    return None, False, "No data returned from Addepar API"
                return data, True, "Data fetched successfully"
            except Exception as e:
                return None, False, f"Failed to parse Excel response: {str(e)}"
        else:
            error_detail = response.text if response.text else "No error details available"
            return None, False, f"Failed to fetch data: {response.status_code}\nDetails: {error_detail}"
    except requests.exceptions.RequestException as e:
        return None, False, f"Connection error: {str(e)}" 



def get_portfolio_data_using_jobs(view_type: str, view_id: str, api_key: str, api_secret: str, firm_id: str, base_url: str, 
                      start_date: str, end_date: str) -> Tuple[bool, str, pd.DataFrame]:
    """Retrieve portfolio data from Addepar API using the Jobs API for large requests."""
    job_id = _create_portfolio_job(view_type, view_id, api_key, api_secret, firm_id, base_url, start_date, end_date)
    if not job_id:
        return False, "Failed to create or retrieve portfolio data job", pd.DataFrame()

    # Poll for job completion and get results
    data = _poll_job_completion(job_id, api_key, api_secret, firm_id, base_url)
    if not data:
        return False, "Failed to retrieve job results", pd.DataFrame()

    try:
        # Save raw JSON response for debugging/reference
        os.makedirs('data/raw', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df = _process_portfolio_data(data)
        return True, "Data retrieved successfully!", df

    except Exception as e:
        return False, f"Error processing data: {str(e)}", pd.DataFrame()

def _create_portfolio_job(view_type: str, view_id: str, api_key: str, api_secret: str, firm_id: str, base_url: str, 
                      start_date: str, end_date: str) -> str:
    """Create a job to retrieve portfolio data."""
    url = f"{base_url}/api/v1/jobs"
    
    params = {
        "data": {
            "type": "jobs",
            "attributes": {
                "job_type": "portfolio_view_results",
                "parameters": {
                    "view_id": view_id,
                    "portfolio_type": "firm",
                    "portfolio_id": "1",
                    "output_type": "json",
                    "start_date": start_date,
                    "end_date": end_date
                }
            }
        }
    }

    try:
        # Create proper headers with content-type
        headers = {
            'Authorization': create_auth_header(api_key, api_secret),
            'Addepar-Firm': firm_id,
            'Accept': 'application/vnd.api+json',
            'Content-Type': 'application/vnd.api+json'
        }
        
        response = requests.post(
            url,
            headers=headers,
            json=params
        )

        print(f"Job creation request URL: {url}")
        print(f"Job creation response status: {response.status_code}")

        if response.status_code == 202:  # 202 Accepted is expected for job creation
            data = response.json()
            return data.get('data', {}).get('id')
        else:
            print(f"Failed to create job: {response.status_code}")
            print(f"Response: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Connection error creating job: {str(e)}")
        return None

def _poll_job_completion(job_id: str, api_key: str, api_secret: str, firm_id: str, base_url: str, max_attempts: int = 30, delay: int = 20) -> Dict:
    """Poll for job completion and retrieve results."""
    url = f"{base_url}/api/v1/jobs/{job_id}"
    
    headers = {
        'Authorization': create_auth_header(api_key, api_secret),
        'Addepar-Firm': firm_id,
        'Accept': 'application/vnd.api+json'
    }
    
    for attempt in range(max_attempts):
        try:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                
                # Check if we got portfolio data instead of job status
                if 'meta' in data and 'data' in data and 'total' in data.get('data', {}).get('attributes', {}):
                    print("\nReceived portfolio data - job is complete")
                    return data
                
                # Otherwise check job status
                status = data.get('data', {}).get('attributes', {}).get('status')
                percent_complete = data.get('data', {}).get('attributes', {}).get('percent_complete')
                print(f"\nJob status: {status} ({percent_complete}% complete). Attempt {attempt + 1}/{max_attempts}")
                
                if status in ["Failed", "Canceled", "Timed Out", "Rejected", "Error Cancelled"]:
                    print(f"Job failed with status: {status}")
                    errors = data.get('data', {}).get('attributes', {}).get('errors')
                    if errors:
                        print(f"Error details: {errors}")
                    return None

                time.sleep(delay)
            else:
                print(f"Error checking job status: {response.status_code}")
                print(f"Response: {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Connection error checking job: {str(e)}")
            return None

    print("Max polling attempts reached")
    return None

def _process_portfolio_data(data: Dict[str, Any]) -> pd.DataFrame:
    """Process the API response into a pandas DataFrame for alts list data."""
    rows = []

    # Navigate to the total children (positions)
    total_data = data.get('data', {}).get('attributes', {}).get('total', {}).get('children', [])
    
    for position in total_data:
        position_name = position.get('name', '')
        position_columns = position.get('columns', {})
        rows.append({
            'Position (Security)': position_name,
            'Position (Owner)': position_columns.get('position', ''),
            'Ownership Type': position_columns.get('ownership_type', ''),
            'Cost Value (USD)': position_columns.get('cost_basis', 0),
            'Mkt Value (USD)': position_columns.get('value', 0),
            'Inception Unfunded Commitment (USD)': position_columns.get('unfunded_commitment', 0),
            'Total Commitments (Since Inception, USD)': position_columns.get('total_commitments', 0),
            'Total Contributions (Since Inception, USD)': position_columns.get('fund_contributions', 0),
            'Capital Returned (Since Inception, USD)': position_columns.get('fund_distributions', 0),
            'Direct Owner ID': position_columns.get('direct_owner_id', ''),
            'Entity ID': position_columns.get('node_id', '')
        })

    return pd.DataFrame(rows) 