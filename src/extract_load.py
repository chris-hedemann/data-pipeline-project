import pandas as pd
import click
# from google.cloud import bigquery 
from google.oauth2 import service_account


@click.command()
@click.option('--sa_path_storage', help="Path to the service account json file for storage")
@click.option('--sa_path_gbq', help="Path to the service account json file for GBQ")
@click.option('--bucket_name', help="The name of the bucket to upload the data to on GCP")
@click.option('--year', help="The year of the taxi data to download")
@click.option('--color', help="The colour of the taxis for which you want to download data")
@click.option('--project_id', help="The project ID on GBQ")
def main(sa_path_storage, sa_path_gbq, project_id, bucket_name, color, year):
    for month in range(1,4):
        df = extract(sa_path_storage, bucket_name, color, year, month=month)
        load(df, sa_path_gbq, project_id, color, year, month=month)

def extract(sa_path_storage, bucket_name, color, year, month):
    """Write and read a blob from GCS using file-like IO"""

    file_name = f"{color}_tripdata_{year}-{month:02}.parquet"

    link = f"gs://{bucket_name}/{color}_taxi/{file_name}"

    df = pd.read_parquet(
        link,
        storage_options={"token": sa_path_storage},
        )
    
    return df

def load(df, sa_path_gbq, project_id, color, year, month):
    """Write dataframe to GCS Biqquery"""

    credentials = service_account.Credentials.from_service_account_file(
        sa_path_gbq,
    )
    table_name = f"{color}_taxi.tripdata_{year}_{month:02d}"

    print(f"Uploading {month:02d}/{year} to Big Query ...")
    df.to_gbq(table_name, 
                   project_id=project_id, 
                   credentials=credentials, 
                   chunksize=10000,
                   progress_bar=True,
                   if_exists='replace')
    print("Success")

if __name__ == "__main__":
    main()