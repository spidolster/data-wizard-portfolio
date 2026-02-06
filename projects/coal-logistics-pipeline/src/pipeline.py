import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CoalLogisticsPipeline:
    """
    ETL Pipeline for Mining Logistics Tracker.
    Handles extraction of raw coal shipment data, transformation for logistics optimization,
    and loading into a Postgres data warehouse.
    """
    
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.engine = self._get_db_engine()
        
    def _get_db_engine(self):
        # Placeholder for DB engine creation
        # connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        # return create_engine(connection_string)
        return None

    def extract(self, source_path: str) -> pd.DataFrame:
        """Extracts data from CSV source."""
        logger.info(f"Extracting data from {source_path}")
        try:
            df = pd.read_csv(source_path)
            return df
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transforms data for logistics analysis."""
        logger.info("Transforming coal logistics data...")
        
        # 1. Type Conversion
        df['departure_time'] = pd.to_datetime(df['departure_time'])
        df['expected_arrival'] = pd.to_datetime(df['expected_arrival'])
        
        # 2. Calculate Expected Transit Duration (Hours)
        df['transit_hours'] = (df['expected_arrival'] - df['departure_time']).dt.total_seconds() / 3600
        
        # 3. Flag High Risk Shipments (Delayed or Long Transit > 48h)
        df['risk_flag'] = df.apply(
            lambda x: 'HIGH' if x['status'] == 'Delayed' or x['transit_hours'] > 48 else 'LOW', 
            axis=1
        )
        
        logger.info(f"Transformation complete. Processed {len(df)} records.")
        return df

    def load(self, df: pd.DataFrame, table_name: str):
        """Loads data into the database."""
        logger.info(f"Loading {len(df)} rows into table: {table_name}")
        if self.engine:
            # df.to_sql(table_name, self.engine, if_exists='append', index=False)
            pass
        else:
            logger.warning("No DB Engine configured. Skipping SQL load.")
            print("\n[Preview of Data to Load]")
            print(df[['shipment_id', 'origin_mine', 'transit_hours', 'risk_flag']].to_markdown(index=False))

    def run_pipeline(self, source_path: str, target_table: str):
        """Orchestrates the ETL process."""
        try:
            logger.info("Starting Coal Logistics ETL Pipeline")
            raw_data = self.extract(source_path)
            clean_data = self.transform(raw_data)
            self.load(clean_data, target_table)
            logger.info("Pipeline completed successfully")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    # Demo Configuration
    config = {
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': '5432',
        'dbname': 'datawizard'
    }
    
    # Path to the sample data we just created
    import os
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(base_dir, 'data', 'raw_shipments.csv')
    
    pipeline = CoalLogisticsPipeline(config)
    pipeline.run_pipeline(data_path, 'shipment_logistics')
