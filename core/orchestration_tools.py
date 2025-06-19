
import logging
import json
from typing import List, Dict, Any
from config.settings import get_settings
from tools.db_tools import get_complete_schema, query_database, get_data_dictionary_columns

logger = logging.getLogger(__name__)
settings = get_settings()
CARDINALITY_THRESHOLD = 25
SAMPLE_LIMIT = 5

def build_m_schema_string(tables: List[str], columns: List[str]) -> str:
    """
    Constructs the M-Schema string based on a focused list of tables and columns.

    This function is a critical part of the pipeline. It:
    1. Normalizes all table and column names to be fully qualified.
    2. Fetches the physical schema (data types, PKs, FKs).
    3. Fetches the semantic schema (descriptions from the data dictionary if available).
    4. Implements "Smart Sampling":
        - For low-cardinality columns (<= CARDINALITY_THRESHOLD), it fetches ALL unique values.
        - For high-cardinality columns, it fetches a small, random sample.
    5. Assembles all this information into the final M-Schema string for the SQLGenerator.

    Args:
        tables (List[str]): List of relevant table names.
        columns (List[str]): List of relevant column names.

    Returns:
        str: A formatted string representing the M-Schema, or an error string if it fails.
    """
    logger.info(f"Building M-Schema for tables: {tables} and columns: {columns}")
    if not tables or not columns:
        return "/* M-Schema Error: No tables or columns were provided to the builder. */"

    try:
        if not settings.DB_SCHEMAS:
            raise ValueError("DB_SCHEMAS is not configured in settings.")
        data_schema_name = settings.DB_SCHEMAS[0]

        # --- Step 1: Normalize all table and column names ---
        normalized_columns = []
        for col in columns:
            parts = col.split('.')
            if len(parts) == 2:
                normalized_columns.append(f"{data_schema_name}.{col}")
            elif len(parts) == 3:
                normalized_columns.append(col)
            else:
                logger.warning(f"Skipping malformed column name during normalization: {col}")
        columns = normalized_columns
        
        tables_to_build = set()
        for t in tables:
            if '.' in t:
                tables_to_build.add(t)
            else:
                tables_to_build.add(f"{data_schema_name}.{t}")
        
        logger.info(f"Normalized tables: {tables_to_build}, columns: {columns}")

        # --- Step 2: Fetch base metadata ---
        full_physical_schema = get_complete_schema()
        column_descriptions_dd = get_data_dictionary_columns(table_names=[t.split('.')[-1] for t in tables_to_build])

        # --- Step 3: Build the M-Schema string ---
        m_schema_parts = [f"【DB_ID】 {data_schema_name}\n【Schema】"]
        
        for fq_table_name in tables_to_build:
            if fq_table_name not in full_physical_schema:
                logger.warning(f"Table '{fq_table_name}' not found in physical schema. Skipping.")
                continue

            table_name_only = fq_table_name.split('.')[-1]
            m_schema_parts.append(f"# Table: {fq_table_name}\n[")
            
            selected_cols_for_this_table = {c.split('.')[-1] for c in columns if c.startswith(fq_table_name)}
            physical_cols = full_physical_schema.get(fq_table_name, [])

            for col_info in physical_cols:
                col_name = col_info['name']
                if col_name not in selected_cols_for_this_table:
                    continue
                
                # Get semantic description
                col_desc = "No description available."
                if table_name_only in column_descriptions_dd:
                    for desc_item in column_descriptions_dd.get(table_name_only, []):
                        if desc_item.get("column_name") == col_name:
                            col_desc = desc_item.get("column_description", col_desc)
                            break
                
                # --- Smart Sampler Logic ---
                sample_values = []
                label = "Sample Values" # Default label
                try:
                    # 1. Get the count of distinct values to decide on a strategy
                    count_query = f'SELECT COUNT(DISTINCT "{col_name}") AS unique_count FROM "{data_schema_name}"."{table_name_only}";'
                    count_result = query_database(count_query)
                    unique_count = count_result[0]['unique_count'] if count_result and 'unique_count' in count_result[0] else 0

                    # 2. Choose strategy based on cardinality
                    if 0 < unique_count <= CARDINALITY_THRESHOLD:
                        # Low cardinality: get all unique values
                        logger.info(f"Fetching all {unique_count} unique values for low-cardinality column: {fq_table_name}.{col_name}")
                        sample_query = f'SELECT DISTINCT "{col_name}" FROM "{data_schema_name}"."{table_name_only}" WHERE "{col_name}" IS NOT NULL ORDER BY "{col_name}";'
                        label = "All Unique Values" # Hint to the LLM that this list is complete
                    else:
                        # High cardinality: get a small sample
                        logger.info(f"Fetching {SAMPLE_LIMIT} samples for high-cardinality column: {fq_table_name}.{col_name} (total unique: {unique_count})")
                        sample_query = f'SELECT DISTINCT "{col_name}" FROM "{data_schema_name}"."{table_name_only}" WHERE "{col_name}" IS NOT NULL LIMIT {SAMPLE_LIMIT};'
                        label = "Sample Values"

                    # 3. Execute the chosen query and format results
                    samples_result = query_database(sample_query)
                    if isinstance(samples_result, list) and samples_result and "error" not in samples_result[0]:
                        raw_values = [list(row.values())[0] for row in samples_result]
                        for val in raw_values:
                            if hasattr(val, 'strftime'):
                                sample_values.append(val.strftime('%Y-%m-%d %H:%M:%S' if ':' in str(val) else '%Y-%m-%d'))
                            else:
                                sample_values.append(str(val))
                except Exception as e:
                    logger.error(f"Failed to get smart samples for {fq_table_name}.{col_name}: {e}")
                    # Keep sample_values as an empty list on error
                # --- End Smart Sampler Logic ---

                # Assemble the final column string
                pk_info = "Primary Key, " if col_info.get('primary_key') else ""
                col_type = col_info['type']
                m_schema_parts.append(f"  ({col_name}:{col_type}, {pk_info}{col_desc}, {label}: {sample_values})")
            
            m_schema_parts.append("]\n")

        # --- Step 4: Add Foreign Key relationships ---
        m_schema_parts.append("【Foreign keys】")
        for fq_table_name in tables_to_build:
             if fq_table_name in full_physical_schema:
                for col_info in full_physical_schema[fq_table_name]:
                    if 'foreign_key' in col_info:
                        fk = col_info['foreign_key']
                        m_schema_parts.append(f"{fq_table_name}.{col_info['name']} = {fk['table']}.{fk['column']}")

        return "\n".join(m_schema_parts)

    except Exception as e:
        logger.error(f"A critical error occurred while building M-Schema: {e}", exc_info=True)
        return f"/* Error: A critical error occurred in the M-Schema builder. Details: {e} */"