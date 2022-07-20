"""
Scrap the data from the UK government website
using beautifulsoup4 and requests
"""
import sys
import os
import json

from typing import TypedDict, List, Union

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class AuthResponse(TypedDict):
    """
    The response from the authenticate function.
    """

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str


def authenticate(role: str) -> Union[AuthResponse, None]:
    """
    Authenticate to aws assuming role using sts
    """
    sts = boto3.client("sts")
    response = sts.assume_role(RoleArn=role, RoleSessionName="Runner")

    # check if the response is valid
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        print(
            f"Error: Could not authenticate to aws with code {response['ResponseMetadata']['HTTPStatusCode']}"
        )
        return None

    # extract key id, secret and token from the response and put it in a dictionary
    credentials = {
        "aws_access_key_id": response["Credentials"]["AccessKeyId"],
        "aws_secret_access_key": response["Credentials"]["SecretAccessKey"],
        "aws_session_token": response["Credentials"]["SessionToken"],
    }
    # return the client
    return credentials


def get_cvs_links() -> List[str]:
    """
    Get the link to the csv file
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    with open(f"{current_dir}/dataFiles.json", encoding="utf8") as file:
        links = json.load(file)
        return links


def convert_csv_to_parquet(link: str) -> str:
    """
    Convert csv to parquet
    """
    # convert csv to parquet using pandas and pyarrow

    # read the bytes to csv using pandas
    df_local = pd.read_csv(link)

    # convert the dataframe to parquet
    table = pa.Table.from_pandas(df_local)

    # delete file if it exists
    if os.path.exists("data.parquet"):
        os.remove("data.parquet")

    pq.write_table(table, "data.parquet")

    # save the parquet file to bytes
    with open("data.parquet", "rb") as file:
        data = file.read()

    return data


def store_data_in_s3(links: List[str], bucket: str, credentials: AuthResponse):
    """
    Store the data in s3
    """
    s3_client = boto3.client("s3", **credentials)
    for link in links:
        # catch the exception if the link is not valid
        try:
            
            # convert csv to parquet
            parquet_file = convert_csv_to_parquet(link)

            # get the file name from the link
            file_name = link.split("/")[-1]

            # remove csv extension and add parquet extension
            file_name = file_name.replace(".csv", ".parquet")

            print(f"Storing {file_name} in {bucket}")
            s3_client.put_object(Bucket=bucket, Key=f"raw/{file_name}", Body=parquet_file)
        except UnicodeDecodeError as error:
            s3_client.put_object(Bucket=bucket, Key=f"dlq/{file_name}/link", Body=link)
            s3_client.put_object(Bucket=bucket, Key=f"dlq/{file_name}/error", Body=str(error))




def main():
    """
    Entry function
    """

    # check if the role is provided as environment variable
    if "AWS_ROLE" not in os.environ:
        print("Error: No role provided")
        sys.exit(1)

    # check if the bucket is provided as environment variable
    if "AWS_BUCKET" not in os.environ:
        print("Error: No bucket provided")
        sys.exit(1)

    # get bucket name from environment variable
    bucket = os.environ["AWS_BUCKET"]

    # get the role from the environment variable
    role = os.environ["AWS_ROLE"]

    # check if the role is valid
    if not role.startswith("arn:aws:iam::"):
        print(f"Error: Invalid role {role}")
        sys.exit(1)

    credentials = authenticate(role)

    if credentials is None:
        sys.exit(1)

    links = get_cvs_links()

    store_data_in_s3(links, bucket, credentials)


if __name__ == "__main__":
    main()
    sys.exit(0)
