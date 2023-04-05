from datetime import datetime, timedelta
import os
import io
from dotenv import load_dotenv
import requests
import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 31),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Set DAG ID and schedule interval
dag = DAG(
    "pokeapi_dag",
    default_args=default_args,
    description="A DAG to extract data from the PokeAPI",
    schedule_interval="@daily",
)


load_dotenv(override=True)

aws_s3_bucket = os.getenv("AWS_S3_BUCKET")

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

s3 = session.client("s3")


def extract_pokemon():
    # retrieve the data from the API
    url = "https://pokeapi.co/api/v2/pokemon?limit=10000"
    response = requests.get(url, timeout=1000)
    if response.status_code == 200:
        data = response.json()["results"]
        pokemon_list = []
        for pokemon in data:
            pokemon_url = pokemon["url"]
            response = requests.get(pokemon_url, timeout=1000)
            if response.status_code == 200:
                pokemon_data = response.json()
                types = [t["type"]["name"] for t in pokemon_data["types"]]
                pokemon_dict = {
                    "id": pokemon_data["id"],
                    "name": pokemon_data["name"],
                    "height": pokemon_data["height"],
                    "weight": pokemon_data["weight"],
                    "types": types,
                }
                pokemon_list.append(pokemon_dict)

        # create a DataFrame from the data
        df = pd.DataFrame(pokemon_list)

        # explode the "types" column and select relevant columns
        df = df.explode("types")

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode()

        s3.put_object(
            Bucket=aws_s3_bucket,
            Key="data/pokemon_info.csv",
            Body=csv_data,
        )
    else:
        print(f"Error: HTTP status code {response.status_code}")


def extract_generations():
    # retrieve the data from the API
    url = "https://pokeapi.co/api/v2/generation?limit=10000"
    response = requests.get(url, timeout=1000)
    if response.status_code == 200:
        data = response.json()["results"]
        generations = []
        for generation in data:
            generation_url = generation["url"]
            response = requests.get(generation_url, timeout=1000)
            if response.status_code == 200:
                pokemon_data = response.json()["pokemon_species"]
                pokemon_names = [pokemon["name"] for pokemon in pokemon_data]
                generations.append(
                    {
                        "generation_name": generation["name"],
                        "pokemon_name": pokemon_names,
                    }
                )

            # create a DataFrame from the data
            df = pd.DataFrame(generations)

            # explode the "pokemon_names" column and select relevant columns
            df = df.explode("pokemon_name")

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue().encode()

            s3.put_object(
                Bucket=aws_s3_bucket,
                Key="data/pokemon_generations.csv",
                Body=csv_data,
            )
    else:
        print(f"Error: HTTP status code {response.status_code}")


def extract_moves():
    # Retrieve the data from the API
    response = requests.get("https://pokeapi.co/api/v2/move?limit=10000", timeout=1000)
    if response.status_code == 200:
        data = response.json()["results"]

        # Create a dictionary to store the data
        moves_dict = {}

        # Loop through the data and add it to the dictionary
        for move in data:
            move_id = int(move["url"].split("/")[-2])
            move_response = requests.get(move["url"], timeout=1000)
            move_data = move_response.json()
            move_name = move_data["name"]
            move_accuracy = move_data["accuracy"]

            # Loop through the list of pokemon that can learn this move
            for pokemon in move_data["learned_by_pokemon"]:
                pokemon_name = pokemon["name"]
                if pokemon_name not in moves_dict:
                    moves_dict[pokemon_name] = []
                moves_dict[pokemon_name].append((move_id, move_name, move_accuracy))

        # Convert the dictionary to a Pandas dataframe
        data = []
        for pokemon_name, moves in moves_dict.items():
            for move in moves:
                data.append((pokemon_name, move[0], move[1], move[2]))
        df = pd.DataFrame(
            data,
            columns=["learned_by_pokemon", "move_id", "move_name", "move_accuracy"],
        )

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode()

        s3.put_object(
            Bucket=aws_s3_bucket,
            Key="data/pokemon_moves.csv",
            Body=csv_data,
        )
    else:
        print(f"Error: HTTP status code {response.status_code}")


def extract_habitats():
    # retrieve the data from the API
    url = "https://pokeapi.co/api/v2/pokemon-habitat?limit=3000"
    response = requests.get(url, timeout=1000)
    if response.status_code == 200:
        data = response.json()["results"]
        habitat_list = []
        for habitat in data:
            habitat_url = habitat["url"]
            response = requests.get(habitat_url, timeout=1000)
            if response.status_code == 200:
                habitat_data = response.json()
                for pokemon in habitat_data["pokemon_species"]:
                    pokemon_dict = {
                        "habitat": habitat_data["name"],
                        "pokemon_name": pokemon["name"],
                    }
                    habitat_list.append(pokemon_dict)

        # create a DataFrame from the data
        df = pd.DataFrame(habitat_list)

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode()

        s3.put_object(
            Bucket=aws_s3_bucket,
            Key="data/pokemon_habitats.csv",
            Body=csv_data,
        )

    else:
        print(f"Error: HTTP status code {response.status_code}")


def extract_base_stats():
    url = "https://pokeapi.co/api/v2/pokemon?limit=10000"
    response = requests.get(url, timeout=1000)
    if response.status_code == 200:
        data = response.json()["results"]
        pokemon_list = []
        for pokemon in data:
            pokemon_url = pokemon["url"]
            response = requests.get(pokemon_url, timeout=1000)
            if response.status_code == 200:
                pokemon_data = response.json()

                for bs in pokemon_data["stats"]:
                    base_stats = bs["base_stat"]
                    base_stats_names = bs["stat"]["name"]

                    pokemon_dict = {
                        "id": pokemon_data["id"],
                        "pokemon_name": pokemon_data["name"],
                        "base_stat_name": base_stats_names,
                        "base_stat": base_stats,
                    }
                    pokemon_list.append(pokemon_dict)

        # create a DataFrame from the data
        df = pd.DataFrame(pokemon_list)

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue().encode()

        s3.put_object(
            Bucket=aws_s3_bucket,
            Key="data/pokemon_base_stats.csv",
            Body=csv_data,
        )

    else:
        print(f"Error: HTTP status code {response.status_code}")


pokemon_extract = PythonOperator(
    task_id="extract_pokemon",
    python_callable=extract_pokemon,
    dag=dag,
)
pokemon_extract.doc_md = "Extract Pokemon Information from PokeAPI"

generations_extract = PythonOperator(
    task_id="extract_generations",
    python_callable=extract_generations,
    dag=dag,
)
generations_extract.doc_md = "Extract Pokemon Generations from PokeAPI"

moves_extract = PythonOperator(
    task_id="extract_moves",
    python_callable=extract_moves,
    dag=dag,
)
moves_extract.doc_md = "Extract Pokemon Moves from PokeAPI"

habitats_export = PythonOperator(
    task_id="extract_habitats",
    python_callable=extract_habitats,
    dag=dag,
)
habitats_export.doc_md = "Extract Pokemon Habitats from PokeAPI"

base_stats_export = PythonOperator(
    task_id="extract_base_stats",
    python_callable=extract_base_stats,
    dag=dag,
)
base_stats_export.doc_md = "Extract Pokemon Base Stats from PokeAPI"

copy_to_redshift = BashOperator(
    task_id="copy_to_redshift",
    bash_command="python /workspaces/data-engineer-zoomcamp-project/airflow/scripts/upload_aws_redshift_etl.py",
    dag=dag,
)
copy_to_redshift.doc_md = "Copy S3 CSV file to Redshift tables"

(
    pokemon_extract
    >> generations_extract
    >> moves_extract
    >> habitats_export
    >> base_stats_export
    >> copy_to_redshift
)
