#!/bin/bash

python3 ingest_data.py \
-user=root \
-password=root \
-host=localhost \
-port=5432 \
-url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-07.csv.gz \
-db=ny_taxi \
-tablename=yellow_taxi_trips