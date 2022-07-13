"""
Testing the scrapper code.
"""

import os

import boto3
from moto import mock_sts, mock_s3

from run import authenticate, convert_csv_to_parquet, store_data_in_s3


@mock_sts
def test_authenticate():
    """
    Test the authenticate function.
    """
    # assert that the function returns a dictionary with the correct keys
    authent = authenticate("arn:aws:iam::123456789012:role/test-role")

    assert isinstance(authent, dict)
    assert "aws_access_key_id" in authent
    assert "aws_secret_access_key" in authent
    assert "aws_session_token" in authent


def test_convert_csv_to_parquet():
    """
    Test the convert_csv_to_parquet function.
    """
    # assert converst the csv to parquet

    link = "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1002058/" \
        "HO_Government_Major_Projects_Portofolio_Data_March_2021.csv"

    parquet = convert_csv_to_parquet(link)

    # delete file if it exists
    if os.path.exists("data.parquet"):
        os.remove("data.parquet")

    assert isinstance(parquet, bytes)


@mock_s3
def test_store_data_in_s3():
    """
    Test the store_data_in_s3 function.
    """

    links = [
        "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1002058/" \
            "HO_Government_Major_Projects_Portofolio_Data_March_2021.csv",
        "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1002101/" \
            "MOJ_Government_Major_Projects_Portofolio_Data_March_2021.csv",
    ]

    conn = boto3.resource("s3", region_name="us-east-1")
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket="testbucket")

    store_data_in_s3(
        links,
        "testbucket",
        {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "aws_session_token": "test",
        },
    )

    # assert that the data was stored in s3
    bucket = conn.Bucket("testbucket")
    assert len(list(bucket.objects.all())) == 2

    # delete file if it exists
    if os.path.exists("data.parquet"):
        os.remove("data.parquet")
