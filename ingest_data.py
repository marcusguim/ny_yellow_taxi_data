import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import os

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    tablename = args.tablename
    url = args.url
  
    csv_name = "output.csv.gz"

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df=next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name=tablename, con=engine, if_exists='replace')

    while True:
        try:
            t_start = time()
            df=next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=tablename, con=engine, if_exists='append')
            t_end = time()
            print(f"Inserted another chunk..., took {(t_end - t_start):.2f} seconds")
        except StopIteration:
            print('Finished!')
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('-user', required=True, help='username for postgres')
    parser.add_argument('-password', required=True, help='password for postgres')
    parser.add_argument('-host', required=True, help='host for postgres')
    parser.add_argument('-port', required=True, help='port for postgres')
    parser.add_argument('-db', required=True, help='database name for postgres')
    parser.add_argument('-tablename', required=True, help='table name for postgres')
    parser.add_argument('-url', required=True, help='csv file url')
    
    args = parser.parse_args()

    main(args)