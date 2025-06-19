from typing import List, Dict, Any, Union, Optional, Tuple
import logging
import json
import psycopg2.sql as sql

from tools.db import get_db_pool, get_db_connection, release_connection
from config.settings import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


def get_schemas() -> List[str]:
    """
    Get the list of schemas configured for the application.
    Returns:
        List[str]: List of schema names
    """
    return settings.DB_SCHEMAS


def get_all_db_objects() -> Dict[str, List[Dict[str, str]]]:
    """
    Get all database objects (tables, views, materialized views) from the configured schemas.

    Returns:
        Dict[str, List[Dict[str, str]]]: Dictionary with keys 'tables', 'views', 'materialized_views'
        containing information about each object.
    """
    schemas = get_schemas()
    schema_list = ','.join(f"'{schema}'" for schema in schemas)

    query = f"""
    SELECT
        table_schema,
        table_name,
        'table' as object_type
    FROM
        information_schema.tables
    WHERE
        table_schema IN ({schema_list})
        AND table_type = 'BASE TABLE'
    UNION ALL
    SELECT
        table_schema,
        table_name,
        'view' as object_type
    FROM
        information_schema.views
    WHERE
        table_schema IN ({schema_list})
    UNION ALL
    SELECT
        schemaname as table_schema,
        matviewname as table_name,
        'materialized_view' as object_type
    FROM
        pg_matviews
    WHERE
        schemaname IN ({schema_list})
    ORDER BY
        table_schema, table_name;
    """

    result = {
        'tables': [],
        'views': [],
        'materialized_views': []
    }

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                obj_info = {
                    'schema': row['table_schema'],
                    'name': row['table_name'],
                    'full_name': f"{row['table_schema']}.{row['table_name']}"
                }

                if row['object_type'] == 'table':
                    result['tables'].append(obj_info)
                elif row['object_type'] == 'view':
                    result['views'].append(obj_info)
                elif row['object_type'] == 'materialized_view':
                    result['materialized_views'].append(obj_info)

    except Exception as e:
        logger.error(f"Error retrieving database objects: {str(e)}")
        # Return empty results on error rather than raising to prevent cascading failures
    finally:
        if conn:
            release_connection(conn)

    return result


def get_complete_schema() -> Dict[str, List[Dict[str, Any]]]:
    """
    Get complete schema information for tables in the configured schemas.

    Returns:
        Dict[str, List[Dict[str, Any]]]: Dictionary mapping table names to lists of
        column information dictionaries.
    """
    schemas = get_schemas()
    schema_list = ','.join(f"'{schema}'" for schema in schemas)

    schema_dict = {}

    conn = None
    try:
        conn = get_db_connection()

        # Get all tables from the specified schemas
        tables_query = f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ({schema_list})
        ORDER BY table_schema, table_name;
        """

        with conn.cursor() as cursor:
            cursor.execute(tables_query)
            tables = cursor.fetchall()

            for table_record in tables:
                schema_name = table_record['table_schema']
                table_name = table_record['table_name']
                qualified_table_name = f"{schema_name}.{table_name}"

                # Get column details for this table
                columns_query = f"""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM
                    information_schema.columns
                WHERE
                    table_schema = %s
                    AND table_name = %s
                ORDER BY
                    ordinal_position;
                """

                cursor.execute(columns_query, (schema_name, table_name))
                columns = cursor.fetchall()

                # Format column information
                columns_info = []
                for col in columns:
                    col_info = {
                        'name': col['column_name'],
                        'type': col['data_type'],
                        'nullable': col['is_nullable'] == 'YES',
                        'default': col['column_default'],
                    }

                    # Add character length for string types if applicable
                    if col['character_maximum_length'] is not None:
                        col_info['max_length'] = col['character_maximum_length']

                    columns_info.append(col_info)

                schema_dict[qualified_table_name] = columns_info

            # Get foreign key constraints for each schema
            for schema_name in schemas:
                fk_query = """
                SELECT
                    tc.table_schema,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s;
                """

                cursor.execute(fk_query, (schema_name,))
                foreign_keys = cursor.fetchall()

                # Add foreign key information to the schema dictionary
                for fk in foreign_keys:
                    table_name = f"{fk['table_schema']}.{fk['table_name']}"
                    foreign_table = f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}"

                    # Ensure the table exists in our dictionary (it might be a view that wasn't captured above)
                    if table_name not in schema_dict:
                        continue

                    # Look for the column in the columns list
                    for col in schema_dict[table_name]:
                        if col['name'] == fk['column_name']:
                            if 'foreign_key' not in col:
                                col['foreign_key'] = {}
                            col['foreign_key'] = {
                                'table': foreign_table,
                                'column': fk['foreign_column_name']
                            }
                            break

    except Exception as e:
        logger.error(f"Error retrieving complete schema: {str(e)}")
        return {}  # Return empty dictionary on error rather than raising
    finally:
        if conn:
            release_connection(conn)

    return schema_dict

def _get_metadata_schema() -> Optional[str]:
    if settings.DB_SCHEMAS and len(settings.DB_SCHEMAS) > 1:
        return settings.DB_SCHEMAS[1] # Second item in the list
    logger.warning("METADATA SCHEMA (DB_SCHEMAS[1]) is not properly configured. DD tables might not be found.")
    return None # Or raise an error, or return a default like 'public'

# --- Helper to get fully qualified DD table name ---
def _get_qualified_dd_identifier(table_name_only: str) -> Optional[sql.Identifier]:
    metadata_schema = _get_metadata_schema()
    if metadata_schema and table_name_only:
        return sql.Identifier(metadata_schema, table_name_only)
    elif table_name_only: # No specific metadata schema, assume table name is enough
        logger.warning(f"No specific metadata schema configured, attempting to use '{table_name_only}' directly.")
        return sql.Identifier(table_name_only)
    return None


def get_data_dictionary_tables() -> List[Dict[str, Any]]:
    """
    Retrieves all table descriptions and their priorities from the DD_TABLE.
    The schema for DD_TABLE is the second schema listed in settings.DB_SCHEMAS.
    The table name is configured via settings.DD_TABLE_NAME_ONLY.
    Expected columns in DD_TABLE: "Table", "Priority", "Table Description".
    Returns a list of dictionaries, each containing these fields for every row.
    """
    if not settings.METADATA_AVAILABLE:
        logger.info("Tool: get_data_dictionary_tables - METADATA_AVAILABLE is false, skipping.")
        return [{"info": "Data dictionary table information not available (flag set to false)."}]

    dd_table_actual_name = settings.DD_TABLE_NAME_ONLY
    dd_table_identifier = _get_qualified_dd_identifier(dd_table_actual_name)

    if not dd_table_identifier:
        logger.error(f"Tool: get_data_dictionary_tables - Could not determine qualified name for DD_TABLE ('{dd_table_actual_name}'). Check METADATA_SCHEMA configuration.")
        return [{"error": f"Configuration error for DD_TABLE name: {dd_table_actual_name}"}]

    # Using sql.Identifier for column names from your DD_TABLE
    query = sql.SQL("SELECT {col_table}, {col_priority}, {col_desc} FROM {dd_table};").format(
        col_table=sql.Identifier("Table"),
        col_priority=sql.Identifier("Priority"),
        col_desc=sql.Identifier("Table Description"),
        dd_table=dd_table_identifier
    )

    results = []
    conn = None
    logger.info(f"Tool: get_data_dictionary_tables - Querying {str(dd_table_identifier)}.")
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            for row in rows:
                results.append({
                    "table_name": row["Table"], # Map to consistent output key
                    "priority": row["Priority"],
                    "table_description": row["Table Description"]
                })
        logger.info(f"Tool: Retrieved {len(results)} table descriptions from {str(dd_table_identifier)}.")
        if not results:
            logger.info(f"Tool: No entries found in {str(dd_table_identifier)}.")
            return [{"info": f"No entries found in data dictionary table '{str(dd_table_identifier)}'."}]
    except Exception as e:
        logger.error(f"Tool: Error in get_data_dictionary_tables querying {str(dd_table_identifier)}: {str(e)}", exc_info=True)
        return [{"error": f"Failed to retrieve from {str(dd_table_identifier)}: {str(e)}"}]
    
    return results


def get_data_dictionary_columns(table_names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Retrieves column information (application table name, column name, description, priority) 
    for a given list of application table names from the DD_COLUMNS table.
    The schema for DD_COLUMNS is the second schema listed in settings.DB_SCHEMAS.
    The table name is configured via settings.DD_COLUMN_NAME_ONLY.
    Input 'table_names' should be a list of strings (e.g., ["patients", "treatments"]).
    Expected columns in DD_COLUMNS: "TABLE", "Column Name", "Priority", "Column Description".
    Output: A dictionary where keys are the input application table names, and values are lists 
            of column dictionaries for that table.
    """
    if not settings.METADATA_AVAILABLE:
        logger.info("Tool: get_data_dictionary_columns - METADATA_AVAILABLE is false, skipping.")
        return {"info": "Data dictionary column information not available (flag set to false)."}

    if not table_names or not isinstance(table_names, list) or not all(isinstance(name, str) for name in table_names):
        logger.warning("Tool: get_data_dictionary_columns - Invalid 'table_names'. Must be a non-empty list of strings.")
        return {"error": "Invalid 'table_names' argument. Must be a non-empty list of table name strings."}

    dd_column_actual_name = settings.DD_COLUMN_NAME_ONLY
    dd_column_identifier = _get_qualified_dd_identifier(dd_column_actual_name)

    if not dd_column_identifier:
        logger.error(f"Tool: get_data_dictionary_columns - Could not determine qualified name for DD_COLUMNS ('{dd_column_actual_name}'). Check METADATA_SCHEMA config.")
        return {"error": f"Configuration error for DD_COLUMNS name: {dd_column_actual_name}"}

    table_names_tuple = tuple(table_names) 

    # Using sql.Identifier for column names from your DD_COLUMNS
    query_template = sql.SQL("""
        SELECT {col_app_table}, {col_col_name}, {col_priority}, {col_col_desc}
        FROM {dd_column_table}
        WHERE {col_app_table} IN %s
        ORDER BY {col_app_table}, {col_priority} DESC, {col_col_name}; 
    """)
    # Note: "TABLE" is a reserved keyword in SQL, so using sql.Identifier is good.
    # "Column Name" and "Column Description" having spaces also necessitates quoting via sql.Identifier.
    query = query_template.format(
        col_app_table=sql.Identifier("Table"),       # Column in DD_COLUMNS storing application table names
        col_col_name=sql.Identifier("Field_Name"),  # Column in DD_COLUMNS storing column names
        col_priority=sql.Identifier("Priority"),     # Column in DD_COLUMNS for priority
        col_col_desc=sql.Identifier('Column Description'), # Column in DD_COLUMNS for description
        dd_column_table=dd_column_identifier
    )
    
    results_by_table: Dict[str, List[Dict[str, Any]]] = {name: [] for name in table_names}
    conn = None
    logger.info(f"Tool: get_data_dictionary_columns for app_tables: {table_names} (from DD table: {str(dd_column_identifier)})")

    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, (table_names_tuple,))
            rows = cursor.fetchall()
            for row in rows:
                app_table_name_from_row = row["Table"] 
                if app_table_name_from_row in results_by_table:
                    results_by_table[app_table_name_from_row].append({
                        # Output keys for the agent
                        "column_name": row["Field_Name"], 
                        "column_description": row['Column Description'],
                        "priority": row["Priority"]
                    })
                else:
                    logger.warning(f"Tool: Row found in {str(dd_column_identifier)} for table '{app_table_name_from_row}' which was not in original request list or casing mismatch. Requested: {table_names}")
        
        retrieved_tables_with_cols = [k for k, v in results_by_table.items() if v]
        if retrieved_tables_with_cols:
            logger.info(f"Tool: Retrieved column descriptions from {str(dd_column_identifier)} for tables: {retrieved_tables_with_cols}.")
        else:
            logger.info(f"Tool: No column descriptions found in {str(dd_column_identifier)} for any of the requested tables: {table_names}.")
        
        # Ensure all requested tables are keys in the output, even if no columns found
        for name in table_names:
             if not results_by_table[name]: # If list is still empty for this requested table
                 results_by_table[name] = [{"info": f"No column descriptions found in data dictionary for table '{name}'."}]

    except Exception as e:
        logger.error(f"Tool: Error in get_data_dictionary_columns querying {str(dd_column_identifier)}: {str(e)}", exc_info=True)
        return {"error": f"Failed to retrieve from {str(dd_column_identifier)}: {str(e)}"}
    return results_by_table




def query_database(query: str) -> List[Dict[str, Any]]:
    """
    Execute a SQL query against the PostgreSQL database.

    Args:
        query (str): SQL query to execute

    Returns:
        List[Dict[str, Any]]: Query results as a list of dictionaries
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

            # Convert rows to dictionaries (should already be dictionaries with RealDictCursor)
            formatted_result = []
            for row in result:
                formatted_result.append(dict(row))

        return formatted_result
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        return [{"error": str(e)}]
    finally:
        if conn:
            release_connection(conn)


def explain_query(query: str) -> List[str]:
    """
    Explain a SQL query execution plan without executing the query.

    Args:
        query (str): SQL query to explain

    Returns:
        List[str]: Explanation of query execution plan
    """
    conn = None
    try:
        # Add EXPLAIN ANALYZE to the beginning of the query if it's not already there
        if not query.lower().strip().startswith("explain"):
            explain_query = f"EXPLAIN {query}"
        else:
            explain_query = query

        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(explain_query)
            result = cursor.fetchall()

            # Format the explain results for readability
            explain_results = []
            for row in result:
                # The EXPLAIN result is in the first column
                first_column = list(row.values())[0]
                explain_results.append(first_column)

        return explain_results
    except Exception as e:
        logger.error(f"Error explaining query: {str(e)}")
        return [f"Error explaining query: {str(e)}"]
    finally:
        if conn:
            release_connection(conn)