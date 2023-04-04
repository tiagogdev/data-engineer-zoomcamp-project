import configparser
import pathlib
import sys
import psycopg2
from psycopg2 import sql

"""
Part of DAG. Upload S3 CSV data to Redshift. Takes one argument of format YYYYMMDD. This is the name of 
the file to copy from S3. Script will load data into temporary table in Redshift, delete 
records with the same post ID from main table, then insert these from temp table (along with new data) 
to main table. This means that if we somehow pick up duplicate records in a new DAG run,
the record in Redshift will be updated to reflect any changes in that record, if any (e.g. higher score or more comments).
"""

# Parse our configuration file
script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/configuration.conf")

# Store our configuration variables
USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_hostname")
PORT = parser.get("aws_config", "redshift_port")
AWS_ACCESS_KEY_ID = parser.get("aws_config", "aws_access_key_id")
AWS_SECRET_ACCESS_KEY = parser.get("aws_config", "aws_secret_access_key")
DATABASE = parser.get("aws_config", "redshift_database")
SCHEMA_NAME = parser.get("aws_config", "redshift_schema")
BUCKET_NAME = parser.get("aws_config", "bucket_name")


# Our S3 files
pokemon_info = f"s3://{BUCKET_NAME}/data/pokemon_info.csv"
pokemon_generations = f"s3://{BUCKET_NAME}/data/pokemon_generations.csv"
pokemon_moves = f"s3://{BUCKET_NAME}/data/pokemon_moves.csv"
pokemon_habitats = f"s3://{BUCKET_NAME}/data/pokemon_habitats.csv"
pokemon_base_stats = f"s3://{BUCKET_NAME}/data/pokemon_base_stats.csv"


# Create Redshift table if it doesn't exist
sql_create_table = sql.SQL(
    """CREATE TABLE IF NOT EXISTS staging.pokemon_info (
        pokemon_id INT PRIMARY KEY,
        name VARCHAR(256),
        height INT,
        weight INT,
        types VARCHAR(256)
    );
    
    CREATE TABLE IF NOT EXISTS staging.pokemon_generations (
        generation_name VARCHAR(256),
        pokemon_name VARCHAR(256)
    );
    
    CREATE TABLE IF NOT EXISTS staging.pokemon_moves (
        learned_by_pokemon VARCHAR(256),
        move_id INT,
        move_name VARCHAR(256),
        move_accuracy DECIMAL
    );
    
    CREATE TABLE IF NOT EXISTS staging.pokemon_habitats (
        habitat VARCHAR(256),
        pokemon_name VARCHAR(256)
    );
    
    CREATE TABLE IF NOT EXISTS staging.pokemon_base_stats (
        pokemon_id INT,
        pokemon_name VARCHAR(256),
        base_stat VARCHAR(256)
    );"""
)

# If ID already exists in table, we remove it and add new ID record during load.
sql_copy_pokemon_info = f"COPY {SCHEMA_NAME}.pokemon_info FROM '{pokemon_info}' CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' IGNOREHEADER 1 DELIMITER ',' CSV;"
sql_copy_pokemon_generations = f"COPY {SCHEMA_NAME}.pokemon_generations FROM '{pokemon_generations}' CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' IGNOREHEADER 1 DELIMITER ',' CSV;"
sql_copy_pokemon_moves = f"COPY {SCHEMA_NAME}.pokemon_moves FROM '{pokemon_moves}' CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' IGNOREHEADER 1 DELIMITER ',' CSV;"
sql_copy_pokemon_habitats = f"COPY {SCHEMA_NAME}.pokemon_habitats FROM '{pokemon_habitats}' CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' IGNOREHEADER 1 DELIMITER ',' CSV;"
sql_copy_pokemon_base_stats = f"COPY {SCHEMA_NAME}.pokemon_base_stats FROM '{pokemon_base_stats}' CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' IGNOREHEADER 1 DELIMITER ',' CSV;"


def main():
    """Upload file form S3 to Redshift Table"""
    rs_conn = connect_to_redshift()
    load_data_into_redshift(rs_conn)


def connect_to_redshift():
    """Connect to Redshift instance"""
    try:
        rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
        )
        return rs_conn
    except Exception as error:
        print(f"Unable to connect to Redshift. Error {error}")
        sys.exit(1)


def load_data_into_redshift(rs_conn):
    """Load data from S3 into Redshift"""
    with rs_conn:
        cur = rs_conn.cursor()
        cur.execute(sql_create_table)
        cur.execute(sql_copy_pokemon_info)
        cur.execute(sql_copy_pokemon_generations)
        cur.execute(sql_copy_pokemon_moves)
        cur.execute(sql_copy_pokemon_habitats)
        cur.execute(sql_copy_pokemon_base_stats)

        # Commit only at the end, so we won't end up
        # with a temp table and deleted main table if something fails
        rs_conn.commit()


if __name__ == "__main__":
    main()
