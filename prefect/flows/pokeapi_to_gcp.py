from pathlib import Path
import pandas as pd
import requests
from prefect_gcp.cloud_storage import GcsBucket
from prefect import flow, task


@task(retries=3)
def extract_pokemon() -> pd.DataFrame:
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

        return df

    else:
        print(f"Error: HTTP status code {response.status_code}")


@task(retries=3)
def extract_generations() -> pd.DataFrame:
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

        return df

    else:
        print(f"Error: HTTP status code {response.status_code}")


@task(retries=3)
def extract_moves() -> pd.DataFrame:
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

        return df

    else:
        print(f"Error: HTTP status code {response.status_code}")


@task(retries=3)
def extract_habitats() -> pd.DataFrame:
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

        return df

    else:
        print(f"Error: HTTP status code {response.status_code}")


@task(retries=3)
def extract_base_stats() -> pd.DataFrame:
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

        return df

    else:
        print(f"Error: HTTP status code {response.status_code}")


@task()
def write_gcs(df: pd.DataFrame, path: Path) -> None:
    """Convert dataframe to parquet snappy and Upload to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-bucket")
    gcp_cloud_storage_bucket_block.upload_from_dataframe(
        df=df, to_path=path, serialization_format="parquet_snappy"
    )
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    df_pokemon = extract_pokemon()
    write_gcs(df_pokemon, "data/pokemon_info")

    df_generations = extract_generations()
    write_gcs(df_generations, "data/pokemon_generations")

    df_moves = extract_moves()
    write_gcs(df_moves, "data/pokemon_moves")

    df_habitats = extract_habitats()
    write_gcs(df_habitats, "data/pokemon_habitats")

    df_base_stats = extract_base_stats()
    write_gcs(df_base_stats, "data/pokemon_base_stats")


if __name__ == "__main__":
    etl_web_to_gcs()
