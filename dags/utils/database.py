from datetime import date
from decimal import Decimal
import os
from typing import List, Dict, Any, Optional, Union
from google.cloud.sql.connector import Connector
import pg8000
from dags.utils.logger import logger

DB_USER: Optional[str] = os.environ.get('DB_USER')
DB_PASS: Optional[str] = os.environ.get('DB_PASS')
DB_NAME: Optional[str] = os.environ.get('DB_NAME')
DB_INSTANCE_CONNECTION_NAME: Optional[str] = os.environ.get('DB_INSTANCE_CONNECTION_NAME')

def get_connection() -> pg8000.Connection:
    """Creates a connection to the PostgreSQL database using Cloud SQL Python Connector."""
    connector = Connector()
    conn: pg8000.Connection = connector.connect(
        DB_INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        timeout=1800  # Set a higher timeout, e.g., 5 minutes
    )
    return conn

def upsert_values(data: List[Dict[str, Any]], columns: List[str], table: str, schema: str, conflict_columns: Union[str, List[str]], batch_size: int = 1000) -> None:
    """
    Upserts data into the specified table using batch inserts.
    
    :param data: List of dictionaries containing the data to upsert
    :param columns: List of column names
    :param table: Target table name
    :param schema: Target schema name
    :param conflict_columns: Column(s) to use in the ON CONFLICT clause. Can be a string for a single column or a list for composite keys.
    :param batch_size: Number of rows to insert in each batch
    """
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        
        # Prepare the conflict columns
        if isinstance(conflict_columns, str):
            conflict_columns = [conflict_columns]
        
        conflict_clause = ", ".join(f'"{col}"' for col in conflict_columns)
        
        # Prepare the SQL statement
        placeholders = ", ".join(["%s"] * len(columns))
        update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in conflict_columns])
        
        insert_stmt = f"""
        INSERT INTO "{schema}"."{table}" ({", ".join(f'"{col}"' for col in columns)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {update_set}
        """
        
        # Convert list of dicts to list of tuples, handling missing keys
        data_tuples = []
        for row in data:
            try:
                data_tuples.append(tuple(row.get(col, None) for col in columns))
            except KeyError as e:
                logger.warning(f"Skipping row due to missing key: {e}")
                logger.warning(f"Problematic row: {row}")
                continue
        
        # Log the SQL statement for debugging (optional)
        logger.info(f"SQL statement: {insert_stmt}")
        logger.info(f"Total rows to upsert: {len(data_tuples)}")
        
        # Execute the statement in batches
        total_upserted = 0
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i+batch_size]
            cur.executemany(insert_stmt, batch)
            conn.commit()
            total_upserted += len(batch)
            logger.info(f"Upserted batch {i//batch_size + 1} ({len(batch)} rows). Total upserted: {total_upserted}")
        
        logger.info(f"Successfully upserted {total_upserted} rows into {schema}.{table}")
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error upserting data: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {e.args}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def execute_sql(query: str) -> None:
    """
    Executes a SQL query.

    :param query: SQL query string to execute
    """
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        logger.info("Successfully executed SQL query")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error executing SQL: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def run_query(query: str) -> List[Dict[str, Any]]:
    """
    Runs a SQL query in batches and returns the results as a list of dictionaries.

    :param query: SQL query string
    :return: List of dictionaries, where each dictionary represents a row of the query result
    """
    conn = None
    cur = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        logger.info(f"Executing query: {query}")
        cur.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in cur.description]
        
        # Fetch all rows in batches
        result = []
        while True:
            rows = cur.fetchmany(1000)  # Fetch 1000 rows at a time
            if not rows:
                logger.info(f"Fetched all batches.")
                break
            result.extend([dict(zip(columns, row)) for row in rows])
            logger.info(f"Fetched {len(rows)} rows for current batch.")
        
        logger.info(f"Query executed successfully. Returned {len(result)} rows.")
        return result
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        logger.error(f"Query was: {query}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def json_serialize(obj):
    """
    JSON serializer for objects not serializable by default

    :param obj: Object to be serialized
    :return: JSON serialized object
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")