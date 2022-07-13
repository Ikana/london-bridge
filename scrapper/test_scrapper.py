"""
Testing the scrapper code.
"""
from textwrap import dedent

import boto3
from moto import mock_sts, mock_s3
import requests
import requests_mock

from run import authenticate, download_links, convert_csv_to_parquet, store_data_in_s3


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


def test_download_links():
    """
    Test the download_links function.
    """
    # assert that the function returns a list of strings
    session = requests.Session()
    adapter = requests_mock.Adapter()
    session.mount("mock://", adapter)

    # convert string to bytes
    content = "test".encode("utf-8")

    adapter.register_uri("GET", "mock://example.com/file1.csv", content=content)
    adapter.register_uri("GET", "mock://example.com/file2.csv", content=content)

    links = ["mock://example.com/file1.csv", "mock://example.com/file2.csv"]

    content = download_links(links, session)

    assert isinstance(content, list)
    assert all(isinstance(link, str) for link in content)


def test_convert_csv_to_parquet():
    """
    Test the convert_csv_to_parquet function.
    """
    # assert converst the csv to parquet

    session = requests.Session()
    adapter = requests_mock.Adapter()
    session.mount("mock://", adapter)

    link = "mock://example.com/file1.csv"

    content = dedent(
        """
    id,name,age
    1,John,20
    2,Jane,21
    """
    ).encode("utf-8")

    adapter.register_uri("GET", link, content=content)

    response = session.get(link)

    parquet = convert_csv_to_parquet(response.content.decode("utf-8"))

    assert isinstance(parquet, bytes)


@mock_s3
def test_store_data_in_s3():
    """
    Test the store_data_in_s3 function.
    """
    # assert that the data was stored in s3
    session = requests.Session()
    adapter = requests_mock.Adapter()
    session.mount("mock://", adapter)

    content = dedent(
        """
    id,name,age
    1,John,20
    2,Jane,21
    """
    ).encode("utf-8")

    adapter.register_uri("GET", "mock://example.com/file1.csv", content=content)
    adapter.register_uri("GET", "mock://example.com/file2.csv", content=content)

    links = ["mock://example.com/file1.csv", "mock://example.com/file2.csv"]

    content = download_links(links, session)

    conn = boto3.resource("s3", region_name="us-east-1")
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket="testbucket")

    store_data_in_s3(
        content,
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
