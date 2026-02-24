import boto3
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import os

load_dotenv()

def extract_from_s3() -> pd.DataFrame:
    """
    Read "movies.csv" from AWS S3 and return pandas DataFrame.
    """
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    response = s3_client.get_object(
        Bucket=os.getenv('AWS_BUCKET_NAME'),
        Key='movies.csv'
    )

    csv_content = response['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_content))

    print(f"Extrected {len(df)} rows from S3.")
    return df


if __name__ == "__main__":
    df = extract_from_s3()
    print(df.head())
    print(f"Columns: {list(df.columns)}")