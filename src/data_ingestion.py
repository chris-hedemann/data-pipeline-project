import click
import pandas as pd
import os
from google.cloud import storage
#pipeline-green-taxi

@click.command()
@click.option('--sa_path', help="Path to the service account json file")
@click.option('--bucket', help="The name of the bucket to upload the data to on GCP")
@click.option('--year', help="The year of the taxi data to download")
@click.option('--color', help="The colour of the taxis for which you want to download data")
def data_ingestion(sa_path, bucket, year, color):
    """Extract taxi data from external cloud storage and 
    upload to Google Cloud Storage bucket"""
    
    for month in range(1,4):
        url = \
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"
        
        file_name = f"{color}_tripdata_{year}-{month:02}.parquet"

        print("Loading file from URL...")
        df = pd.read_parquet(url)

        print("Uploading to file GCP ...")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        client = storage.Client()
        bucket = client.get_bucket(bucket)
        bucket.blob(f'{color}_taxi/{file_name}').upload_from_string(
            df.to_parquet(), "text/parquet"
            )
        print("Success")


if __name__ == "__main__":
    data_ingestion()