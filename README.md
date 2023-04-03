# Overview
This project is focused on analyzing Pokemon data. The goal is to collect data from various sources, store it in a data lake, transform it, and load it into a data warehouse for analysis and visualization.


The data is collected from the [PokeAPI](https://pokeapi.co/) using Python scripts. The data is then stored in a Google Cloud Storage bucket, and transformed using Google Cloud Dataflow. The transformed data is then loaded into Google BigQuery for analysis and visualization.