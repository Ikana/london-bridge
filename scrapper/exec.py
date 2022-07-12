"""
Scrap the data from the UK government website
using beautifulsoup4 and requests
"""
import boto3


def main():
    """
    Entry function
    """
    sts = boto3.client("sts")
    response = sts.get_caller_identity()

    print(response)


if __name__ == "__main__":
    main()
    exit(0)
