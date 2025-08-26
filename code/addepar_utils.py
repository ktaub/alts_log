import json
import requests
import base64
from typing import Tuple
import pandas as pd
from dotenv import load_dotenv
import os
from io import BytesIO
import warnings

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
            data = pd.read_excel(xlsx_data)
            return data, True, "Data fetched successfully"
        else:
            error_detail = response.text if response.text else "No error details available"
            return None, False, f"Failed to fetch data: {response.status_code}\nDetails: {error_detail}"
    except requests.exceptions.RequestException as e:
        return None, False, f"Connection error: {str(e)}" 