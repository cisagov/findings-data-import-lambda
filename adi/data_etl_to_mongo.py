#!/usr/bin/env python3

"""data-etl-to-mongo: A tool for extracting, transforming
and loading JSON data to Mongo.

The source data is a JSON file stored in an AWS S3 bucket.
The destination of the data is a Mongo database.

Usage:
  data-etl-to-mongo --s3-bucket=BUCKET --data-filename=FILE --db-hostname=HOST [--db-port=PORT] [--log-level=LEVEL]
  data-etl-to-mongo (-h | --help)

Options:
  -h --help                   Show this message.
  --s3-bucket=BUCKET          The AWS S3 bucket containing the data
                              file.
  --data-filename=FILE        The name of the file containing the
                              data in the S3 bucket above.
  --db-hostname=HOST          The hostname that has the database to store
                              the data in.
  --db-port=PORT              The port that the database server is
                              listening on. [default: 27017]
  --log-level=LEVEL           If specified, then the log level will be set to
                              the specified value.  Valid values are "debug",
                              "info", "warning", "error", and "critical".
                              [default: warning]
"""

# Standard libraries
import json
import logging
import os
import tempfile

# Third-party libraries (install with pip)
from boto3 import client as boto3_client
import docopt
from pymongo import MongoClient

# Local library
from adi import __version__

def import_data(
    s3_bucket=None,
    data_filename=None,
    db_hostname=None,
    db_port="27017",
    log_level="warning"
):
    """Ingest data from a JSON file in an S3 bucket to a database.

    Parameters
    ----------
    s3_bucket : str
        The AWS S3 bucket containing the data file.

    data_filename : str
        The name of the file containing the data in the S3 bucket
        above.

    db_hostname : str
        The hostname that has the database to store the data in.

    db_port : str
        The port that the database server is listening on. [default: 27017]

    log_level : str
        If specified, then the log level will be set to the specified value.
        Valid values are "debug", "info", "warning", "error", and "critical".
        [default: warning]

    Returns
    -------
    bool : Returns a boolean indicating if the data import was
    successful.

    """
    # Boto3 clients for S3 and SSM
    s3_client = boto3_client("s3")

    # Securely create a temporary file to store the JSON data in
    temp_file_descriptor, temp_data_filepath = tempfile.mkstemp()

    try:
        # Fetch data file from S3 bucket
        s3_client.download_file(
            Bucket=s3_bucket, Key=data_filename, Filename=f"{temp_data_filepath}"
        )
        logging.info(f"Retrieved {data_filename} from S3 bucket {s3_bucket}")

        # Load data JSON
        with open(f"{temp_data_filepath}") as data_json_file:
            json_data = json.load(data_json_file)
        logging.info(f"JSON data loaded from {data_filename}")

        db_connection = MongoClient(host='mongodb://54.227.104.196:27017', tz_aware=True)
        db = db_connection['test-db']
        logging.info(
            f"DB connection set up to {db_hostname}:{db_port}/test-db"
        )

        # Iterate through data and save each record to the database
        for item in json_data:
            db.testTable.insert_one(item)

        logging.info(
            f"{len(json_data)} documents "
            "successfully inserted in database"
        )

        # Delete data object from S3 bucket
        s3_client.delete_object(Bucket=s3_bucket, Key=data_filename)
        logging.info(f"Deleted {data_filename} from S3 bucket {s3_bucket}")
    finally:
        # Delete local temp data file regardless of whether or not
        # any exceptions were thrown in the try block above
        os.remove(f"{temp_data_filepath}")
        logging.info(f"Deleted temporary {data_filename} from local filesystem")

    return True


def main():
    """Set up logging and call the import_data function."""
    # Parse command line arguments
    args = docopt.docopt(__doc__, version=__version__)

    # Set up logging
    log_level = args["--log-level"]
    try:
        logging.basicConfig(
            format="%(asctime)-15s %(levelname)s %(message)s", level=log_level.upper()
        )
    except ValueError:
        logging.critical(
            f'"{log_level}" is not a valid logging level.  Possible values '
            "are debug, info, warning, error, and critical."
        )
        return 1

    result = import_data(
        args["--s3-bucket"],
        args["--data-filename"],
        args["--db-hostname"],
        args["--db-port"],
        args["--log-level"]
    )

    # Stop logging and clean up
    logging.shutdown()

    return result


if __name__ == "__main__":
    main()
