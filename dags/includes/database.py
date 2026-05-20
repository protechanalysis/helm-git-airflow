import logging
import io
import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from util.config import postgres_conn_id, indicators_column_names, database_table_name, json_folder
from includes.extraction import validate_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def create_metric_table() -> None:
    """
    Create PostgreSQL table for ECOWAS indicators with dynamic schema.
    
    Builds table schema with fixed columns (country_name, country_iso3, year)
    and dynamically adds indicator columns from configuration. Uses composite
    primary key on (country_iso3, year).
    
    Returns:
        None
    
    Raises:
        Exception: If table creation fails. Error details are logged with traceback.
    """
    # Define fixed base columns for the table
    column_definitions = [
        "country_name TEXT",
        "country_iso3 VARCHAR(3)",
        "year INTEGER" 
    ]
    
    # Add all indicator columns dynamically from config
    for col_name in indicators_column_names.values():
        column_definitions.append(f"{col_name} FLOAT")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_table_name} (
        {', '.join(column_definitions)},
        PRIMARY KEY (country_iso3, year)
    );
    """

    logging.info(f"Preparing to create table '{database_table_name}' if it does not exist.")

    try:
        # Initialize Postgres connection
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)  
                conn.commit()
                logging.info("Metric table created successfully")
    except Exception as e:
        logging.error(f"Error creating metric table: {e}", exc_info=True)
        raise


def load_dataframe_to_postgres() -> None:
    """
    Load validated DataFrame into PostgreSQL using upsert strategy.
    
    Uses a staging table and COPY command for efficient bulk loading, then
    merges data into the final table with ON CONFLICT handling to update
    existing records or insert new ones based on (country_iso3, year) key.
    
    Returns:
        None
    
    Raises:
        Exception: If database operations fail during staging, copy, or merge.
    """
    df = validate_data()
    try:
        # Open Postgres connection
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                logging.info("loading data into database")
                staging_table = f"{database_table_name}_staging"
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")
                # Create a temporary staging table with same structure as final table
                cursor.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {database_table_name} INCLUDING ALL)")

                buffer = io.StringIO()
                # Export DataFrame to CSV format without headers
                df.to_csv(buffer, index=False, header=False)
                buffer.seek(0)
                cursor.copy_expert(f"COPY {staging_table} FROM STDIN WITH CSV", buffer)
                logging.info(f"Copied {len(df)} rows to staging table")

                merge_sql = f"""
                INSERT INTO {database_table_name}
                SELECT * FROM {staging_table}
                ON CONFLICT (country_iso3, year) DO UPDATE
                SET
                    country_name = EXCLUDED.country_name,
                    {', '.join([
                        f"{col} = EXCLUDED.{col}"
                        for col in df.columns
                        if col not in ['country_iso3','year','country_name']
                    ])};
                """
                cursor.execute(merge_sql)
                logging.info("Merge operation completed successfully")

            conn.commit()
            logging.info(f"Data successfully loaded to {database_table_name}")
    except Exception as e:
        logging.error(f"Database operation failed: {e}", exc_info=True)
        raise

    try:
        logging.info(f"Cleaning up temporary file: {json_folder}")
        if os.path.exists(json_folder):
            os.remove(json_folder)
            logging.info(f"Cleaned up temporary file: {json_folder} successful")
    except Exception as e:
        logging.warning(f"Failed to delete temporary file {json_folder}: {e}")
        raise
