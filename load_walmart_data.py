import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
DB_PARAMS = {
    "user": "dpande",
    "password": "1234",
    "host": "localhost",
    "port": "5432",
    "dbname": "mydb" # Database where the schema will be created
}

# Use a specific schema for this M5 dataset to keep it organized
TARGET_SCHEMA = "walmart_schema" 

# Directory containing your three CSV files
CSV_DIRECTORY = "./walmart_data" 

# Action to take if tables already exist
IF_TABLE_EXISTS = 'replace' 
# --- END OF CONFIGURATION ---

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("m5_data_loader.log", mode='w'),
        logging.StreamHandler()
    ]
)

def main():
    """
    Transforms and loads the M5 competition data into a relational PostgreSQL schema,
    focusing on melting the sales data and establishing all necessary keys.
    """
    logging.info("Starting M5 data transformation and loading process.")
    
    db_url = f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
    engine = create_engine(db_url)

    csv_path = Path(CSV_DIRECTORY)
    if not csv_path.is_dir():
        logging.error(f"Error: CSV directory not found: '{CSV_DIRECTORY}'")
        return

    # --- Step 1: Read all source CSVs ---
    try:
        logging.info("Reading source CSV files into memory...")
        calendar_df = pd.read_csv(csv_path / 'calendar.csv', parse_dates=['date'])
        sell_prices_df = pd.read_csv(csv_path / 'sell_prices.csv')
        sales_eval_df = pd.read_csv(csv_path / 'sales_train_evaluation.csv')
        logging.info("All CSV files read successfully.")
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}. Make sure 'calendar.csv', 'sell_prices.csv', and 'sales_train_evaluation.csv' are in '{CSV_DIRECTORY}'.")
        return

    # --- Step 2: Melt the sales data to get 'd' and 'sales' columns ---
    logging.info("Transforming 'sales_train_evaluation.csv' from wide to long format...")
    id_vars = ['id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id']
    day_vars = [col for col in sales_eval_df.columns if col.startswith('d_')]
    
    sales_long_df = pd.melt(
        sales_eval_df,
        id_vars=id_vars,
        value_vars=day_vars,
        var_name='d',
        value_name='sales'
    )
    # Filter out days with zero sales to reduce table size, which is a common practice for this dataset
    sales_long_df = sales_long_df[sales_long_df['sales'] > 0]
    logging.info(f"Sales data transformed. The new 'sales' table has {len(sales_long_df)} rows (only includes non-zero sales days).")
    
    # Create a dictionary of the final DataFrames to be loaded
    final_dataframes = {
        "calendar": calendar_df,
        "sell_prices": sell_prices_df,
        "sales": sales_long_df
    }

    # --- Step 3: Load data and create keys in a single transaction ---
    try:
        with engine.connect() as connection:
            # Begin a transaction
            with connection.begin(): 
                logging.info(f"Ensuring schema '{TARGET_SCHEMA}' exists...")
                connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

                # Load the tables
                for table_name, df in final_dataframes.items():
                    logging.info(f"Loading data into table: '{TARGET_SCHEMA}.{table_name}'...")
                    df.to_sql(
                        name=table_name,
                        con=connection, # Use the connection within the transaction
                        schema=TARGET_SCHEMA,
                        if_exists=IF_TABLE_EXISTS,
                        index=False,
                        chunksize=20000,
                        method='multi'
                    )
                    logging.info(f"Successfully loaded '{table_name}'.")

                # --- Step 4: Add Primary Keys and Foreign Keys ---
                logging.info("Adding Primary and Foreign Keys to establish relational integrity...")

                # Add Primary Key to 'calendar' so it can be referenced
                connection.execute(text(f"ALTER TABLE {TARGET_SCHEMA}.calendar ADD CONSTRAINT pk_calendar PRIMARY KEY (d);"))
                logging.info("-> Added Primary Key (d) to 'calendar' table.")

                # Add Composite Primary Key to 'sell_prices'
                connection.execute(text(f"""
                    ALTER TABLE {TARGET_SCHEMA}.sell_prices 
                    ADD CONSTRAINT pk_sell_prices PRIMARY KEY (store_id, item_id, wm_yr_wk);
                """))
                logging.info("-> Added Composite Primary Key (store_id, item_id, wm_yr_wk) to 'sell_prices' table.")
                
                # REQUIREMENT 1: Foreign Key from 'sales' to 'calendar' using 'd'
                # This is the key link between sales events and calendar dates.
                connection.execute(text(f"""
                    ALTER TABLE {TARGET_SCHEMA}.sales 
                    ADD CONSTRAINT fk_sales_calendar_d 
                    FOREIGN KEY (d) REFERENCES {TARGET_SCHEMA}.calendar(d);
                """))
                logging.info("-> Added Foreign Key from sales(d) to calendar(d).")

                # REQUIREMENT 2: Linking 'calendar' and 'sell_prices'
                # This is an indirect relationship that the agent must infer through a JOIN.
                # To help the agent, we can add a FK from 'sell_prices' to 'calendar' on 'wm_yr_wk'.
                # Note: 'wm_yr_wk' is not unique in calendar, so we can't make it a PK.
                # But we can still create an index to speed up joins.
                connection.execute(text(f"CREATE INDEX IF NOT EXISTS idx_calendar_wm_yr_wk ON {TARGET_SCHEMA}.calendar (wm_yr_wk);"))
                logging.info("-> Added Index on calendar(wm_yr_wk) to speed up joins with sell_prices.")

            # The transaction is automatically committed here if everything succeeds
            logging.info("All tables loaded and keys created successfully. Transaction committed.")

    except Exception as e:
        logging.error(f"A critical error occurred. The transaction was rolled back.", exc_info=True)
    
    logging.info("--- SCRIPT FINISHED ---")


if __name__ == "__main__":
    main()
