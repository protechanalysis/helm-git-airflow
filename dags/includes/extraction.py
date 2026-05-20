import requests
import pandas as pd
import pandera as pa
import logging
import json
import os
from time import sleep  
from datetime import datetime
from pathlib import Path 
from util.config import ecowas_country, indicators, start_year, end_year, indicators_column_names, json_folder
from includes.validation import get_wide_schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_world_bank_data() -> json:
    """
    Extract World Bank indicator data for ECOWAS countries and save to JSON.
    
    Queries the World Bank API for multiple indicators across ECOWAS countries
    within the specified date range. All data is aggregated and saved to a file.
    
    Returns:
        json: List of all indicator data records, or None if extraction fails.
    
    """
    all_data = []
    country_str = ";".join(ecowas_country)
    logging.info(f"Beginning data extraction for {len(ecowas_country)} countries.")

    for indicator_code, indicator_name in indicators.items():

        # Construct the World Bank API URL
        url = (
            f"http://api.worldbank.org/v2/country/{country_str}/indicator/{indicator_code}"
            f"?format=json&date={start_year}:{end_year}&per_page=1000"
        )
        logging.info(f"Fetching data for indicator: {indicator_name}")

        try:
            # Make GET request to API with a 30s timeout safeguard
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise exception if HTTP status code indicates error

            data = response.json()

            # World Bank API responses are a list: [metadata, data]. We want index 1.
            if len(data) > 1 and data[1]:
                all_data.extend(data[1]) 
            else:
                logging.warning(f"No data returned for indicator: {indicator_code}")

        # Handle various error types separately for clearer debugging
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred for {indicator_code}: {http_err}")
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error for {indicator_code}: {req_err}")
        except ValueError as json_err:
            logging.error(f"JSON decode error for {indicator_code}: {json_err}")

        sleep(0.5)
    logging.info("Data extraction completed")
    if not all_data:
        logging.error("Extraction failed. No data was retrieved from the API.")
        return None

    # Save raw data 
    logging.info(f"Saving raw extracted data to {json_folder}")
    os.makedirs("/opt/airflow/tmp", exist_ok=True)

    # Dump JSON into configured landing file
    with open(json_folder, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=4)


def transform_to_dataframe() -> pd.DataFrame:
    """
    Transform raw World Bank JSON data into a clean wide-format DataFrame.
    
    Loads the previously extracted JSON data, flattens nested structures,
    converts to DataFrame, and pivots from long to wide format where each
    row represents a country-year and columns represent different indicators.
    
    Returns:
        pd.DataFrame: Wide-format DataFrame with countries and years as rows,
                        indicators as columns. Empty DataFrame if raw data is empty.
    
    """
    logging.info(f"Reading raw data from {json_folder}")

    # Load previously saved JSON file
    with open(json_folder, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)

    # Handle case where file exists but is empty
    if not raw_data:
        logging.warning("Raw data file is empty. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Flatten JSON entries into row dictionaries
    records = []
    for entry in raw_data:
        records.append({
            "country_name": entry.get("country", {}).get("value"),          
            "country_iso3": entry.get("countryiso3code"),              
            "year": entry.get("date"),  
            "indicator_code": entry.get("indicator", {}).get("id"), 
            "indicator_name": indicators.get(entry.get("indicator", {}).get("id")), 
            "value": entry.get("value")                                   
        })

    # Convert list of dicts into DataFrame
    df = pd.DataFrame(records)

    logging.info("Cleaning and transforming data")

    # Convert year and value fields into numeric, coercing invalid entries to NaN
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    logging.info("Pivoting data from long to wide format")

    # Pivot: each row is (country, iso, year), each column is an indicator
    df_wide = df.pivot_table(
        index=["country_name", "country_iso3", "year"],
        columns="indicator_code",
        values="value"
    ).reset_index()

    df_wide.columns.name = None

    logging.info("Renaming columns for clarity")

    df_wide.rename(columns=indicators_column_names, inplace=True)

    logging.info(f"Successfully transformed data. Final shape: {df.shape}")
    return df_wide


def validate_data() -> pd.DataFrame:
    """
    Validate transformed DataFrame against the wide-format quality schema.
    
    Runs the transformation pipeline to get fresh data, then validates it
    using Pandera schema rules. Logs all validation errors if checks fail.
    
    Returns:
        pd.DataFrame: Validated DataFrame that passes all schema checks.
    
    Raises:
        pa.errors.SchemaErrors: If data fails validation. Failure cases are
                                logged before raising the exception.
    """
    logging.info("Validating data against the WIDE-FORMAT quality schema")

    try:
        schema = get_wide_schema()

        # Re-run transformation pipeline to get fresh DataFrame
        df = transform_to_dataframe()
        validated_df = schema.validate(df, lazy=True)

        logging.info("Data validation successful.")
        return validated_df

    except pa.errors.SchemaErrors as err:
        # If validation fails, log all errors for debugging
        logging.error("Data validation failed. See failure cases below.")
        logging.error(err.failure_cases)
        raise err 
