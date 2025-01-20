import os
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)

def create_session_with_retry():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[408, 429, 500, 502, 503, 504],
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def create_provider_list_schema():
    return {
        'app': None,
        'application_id': None,
        'provider': None,
        'placement': None
    }

def create_provider_statistic_schema():
    return {
        'date': None,
        'impressions': None,
        'clicks': None,
        'conversions': None,
        'earnings': None,
        'currency': None,
        'sdk_bootups': None,
        'daily_unique_viewers': None,
        'daily_unique_conversions': None,
        'e_cpm': None,
        'platform': None,
        'coin_sum': None
    }

def process_tapjoy_data(data):
    reports = data['data']['publisher']['placementNameAndAppId']['insights']['reports'][0]
    timestamps = data['data']['publisher']['placementNameAndAppId']['insights']['timestamps']
    
    results = []
    for i in range(len(timestamps)):
        result = create_provider_statistic_schema()
        result.update({
            'date': timestamps[i][:10],
            'impressions': reports['impressions'][i],
            'clicks': reports['clicks'][i],
            'conversions': reports['conversions'][i],
            'earnings': reports['earnings'][i],
            'currency': 'USD',
            'daily_unique_viewers': reports['dailyUniqueViewers'][i],
            'daily_unique_conversions': reports['dailyUniqueConversions'][i],
        })
        results.append(result)
    return results

def process_adjoe_data(data):
    results = []
    for row in data:
        result = create_provider_statistic_schema()
        result.update({
            'date': row['Date'],
            'impressions': row['OfferwallImpressions'],
            'earnings': row['Revenue'],
            'currency': row['Currency'],
            'sdk_bootups': row['SDKBootups'],
            'platform': row['Platform'],
            'e_cpm': row['eCPM'],
            'coin_sum': row['CoinSum'],
        })
        results.append(result)
    return results

def process_provider_data(data):
    results = []
    for row in data:
        result = create_provider_list_schema()
        result.update({
            'app': row['app'],
            'application_id': row['applicationId'],
            'provider':  row['provider'],
            'placement': row['placement']
        })
        results.append(result)
    return results

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_provider_data() -> List[Dict[str, Any]]:
    session = create_session_with_retry()
    
    base_url = Variable.get("api_base_url")
    auth = Variable.get("api_auth")
    
    url = f"{base_url}/pabblsupport/demo/applicants/offerproviders/list"
    headers = {
        'Authorization': f'Basic {auth}'
    }
    
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        return process_provider_data(data)
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_provider_statistics(provider: str, placement: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    session = create_session_with_retry()
    
    base_url = Variable.get("api_base_url")
    auth = Variable.get("api_auth")
    
    url = f"{base_url}/pabblsupport/demo/applicants/offerproviders/statistics"
    params = {
        'provider': provider,
        'placement': placement,
        'fromDate': start_date,
        'toDate': end_date
    }
    headers = {
        'Authorization': f'Basic {auth}'
    }
    
    try:
        response = session.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if provider == 'Tapjoy':
            return process_tapjoy_data(data)
        elif provider == 'Adjoe':
            return process_adjoe_data(data)
        else:
            return {"error": "Unsupported provider"}
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise


def etl_provider_data() -> None:
    try:
        data = fetch_provider_data()
        logger.info(f"{len(data)} records retrieved")
        
        if not data:
            logger.warning(f"No data received")
            return
            
        df = pd.DataFrame(data)
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # create table
        logger.info("Creating table if not exists")
        df.to_sql('provider_lists', engine, if_exists='replace', index=False, schema='staging')
        
        logger.info(f"Successfully stored {len(df)} records")
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise


def etl_provider_statistic(provider: str, placement: str, execution_date: datetime, **context) -> None:
    date_str = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = context["params"]["start_date"] if context["params"]["start_date"] else date_str
    end_date = context["params"]["end_date"] if context["params"]["end_date"] else date_str

    try:
        data = fetch_provider_statistics(provider, placement, start_date, end_date)
        logger.info(f"{len(data)} records retrieved")
        
        if not data:
            logger.warning(f"No data received for {provider} on {date_str}")
            return
            
        df = pd.DataFrame(data)
        
        df['provider'] = provider
        df['placement'] = placement
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # create table
        logger.info("Creating table if not exists")
        df.head(0).to_sql('provider_statistics', engine, if_exists='append', index=False, schema='staging')

        # delete existing data which date, provider, and placement exists in df
        logger.info("Deleting before insert")
        with engine.connect() as conn:
            with open(os.path.abspath(__file__ + f"/../../query/etl/delete_data_before_insert.sql"), 'r') as file:
                delete_query = file.read()
            records = [
                (row['date'].strftime('%Y-%m-%d'), row['provider'], row['placement'])
                for _, row in df[['date', 'provider', 'placement']].iterrows()
            ]
            
            # Dynamically format the `VALUES` placeholders
            value_placeholders = ', '.join([f"('{row[0]}', '{row[1]}', '{row[2]}')" for row in records])
            formatted_query = delete_query % value_placeholders
            
            # Execute the query
            conn.execute(text(formatted_query))
        
        # insert data
        logger.info("Inserting data")
        df.to_sql('provider_statistics', engine, if_exists='append', index=False, method='multi', chunksize=1000, schema='staging')
        
        logger.info(f"Successfully stored {len(df)} records for {provider} on {date_str}")
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise
