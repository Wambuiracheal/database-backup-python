import psycopg2
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    filename="replica_monitor.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("DATABASE_NAME"),
    "user": os.getenv("DATABASE_USER"),
    "password": os.getenv("DATABASE_PASSWORD"),
    "host": os.getenv("DATABASE_HOST"),
    "port": os.getenv("DATABASE_PORT")
}

REPLICA_CONFIG = {
    "dbname": os.getenv("REPLICA_DATABASE_NAME"),
    "user": os.getenv("REPLICA_USER"),
    "password": os.getenv("REPLICA_PASSWORD"),
    "host": os.getenv("REPLICA_HOST"),
    "port": os.getenv("REPLICA_PORT")
}


def create_replica():
    """Create a simple replica (backup/restore method)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Example: Copying data (adjust based on DB type)
        cursor.execute("CREATE TABLE IF NOT EXISTS replica_table AS TABLE main_table;")

        conn.commit()
        conn.close()
        logging.info("Replica created successfully.")

    except Exception as e:
        logging.error(f"Replica creation failed: {e}")


def compare_databases():
    """Compare main and replica database tables."""
    try:
        conn_main = psycopg2.connect(**DB_CONFIG)
        conn_replica = psycopg2.connect(**REPLICA_CONFIG)
        cursor_main = conn_main.cursor()
        cursor_replica = conn_replica.cursor()

        # Example: Compare row counts
        cursor_main.execute("SELECT COUNT(*) FROM main_table;")
        main_count = cursor_main.fetchone()[0]

        cursor_replica.execute("SELECT COUNT(*) FROM replica_table;")
        replica_count = cursor_replica.fetchone()[0]

        if main_count == replica_count:
            logging.info("Databases match!")
        else:
            logging.warning(f"Mismatch detected! Main: {main_count}, Replica: {replica_count}")

        conn_main.close()
        conn_replica.close()

    except Exception as e:
        logging.error(f"Comparison failed: {e}")


if __name__ == "__main__":
    create_replica()
    compare_databases()
