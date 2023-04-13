from io import BytesIO
import os
import pandas as pd
from prefect_gcp import GcpCredentials, GcsBucket
import pyarrow.parquet as pq
from prefect import flow, task


@task()
def get_df_from_parquet(file_path: str) -> pd.DataFrame:
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-bucket")

    with BytesIO() as buf:
        gcp_cloud_storage_bucket_block.download_object_to_file_object(file_path, buf)

        # Download the Parquet file as a stream
        # stream = io.BytesIO()
        # buf.download_to_file(stream)
        # stream.seek(0)

        # Convert the Parquet file to a Pandas dataframe
        table = pq.read_table(buf, use_threads=True)
        df = table.to_pandas()

    return df


@task()
def write_bq(df: pd.DataFrame, table_name: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("gcp-creds")

    print(table_name)
    df.to_gbq(
        destination_table=f"pokemon_data_all.{table_name}",
        project_id="subtle-sublime-383512",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(file_paths: list[str]):
    """Main ETL flow to load data into Big Query"""
    for path in file_paths:
        df = get_df_from_parquet(path)
        file_name = os.path.basename(path)
        write_bq(df, os.path.splitext(file_name)[0].split(".")[0])


if __name__ == "__main__":
    file_paths = [
        "data/pokemon_info.snappy.parquet",
        "data/pokemon_generations.snappy.parquet",
        "data/pokemon_moves.snappy.parquet",
        "data/pokemon_habitats.snappy.parquet",
        "data/pokemon_base_stats.snappy.parquet",
    ]

    etl_gcs_to_bq(file_paths)
