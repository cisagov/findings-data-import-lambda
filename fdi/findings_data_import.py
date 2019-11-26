#!/usr/bin/env python3

"""findings_data_import: A tool for extracting, transforming and loading JSON data to Mongo.

The source data is a JSON file stored in an AWS S3 bucket.
The destination of the data is a Mongo database.

Usage:
  findings_data_import --s3-bucket=BUCKET --data-filename=FILE --db-hostname=HOST --field-map=MAP --ssm-db-name=DB --ssm-db-user=USER --ssm-db-password=PASSWORD [--save-failed=FAILED] [--save-succeeded=SUCCEEDED] [--db-port=PORT] [--log-level=LEVEL]
  findings_data_import (-h | --help)

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
  --field-map=MAP             The S3 key for the JSON file containing a map of
                              incoming field names to what they should be for
                              the database.
  --save-failed=FAILED        The directory name used for storing unsuccessfully
                              processed files. [default: True]
  --save-succeeded=SUCCEEDED  The directory name used for storing successfully
                              processed files. [default: False]
  --ssm-db-name=DB            The name of the parameter in AWS SSM that holds
                              the name of the database to store the assessment
                              data in.
  --ssm-db-user=USER          The name of the parameter in AWS SSM that holds
                              the database username with write permission to
                              the assessment database.
  --ssm-db-password=PASSWORD  The name of the parameter in AWS SSM that holds
                              the database password for the user with write
                              permission to the assessment database.
  --log-level=LEVEL           If specified, then the log level will be set to
                              the specified value.  Valid values are "debug",
                              "info", "warning", "error", and "critical".
                              [default: warning]
"""

# Standard libraries
from datetime import datetime
import json
import logging
import os
import re
import tempfile
import urllib

# Third-party libraries (install with pip)
from boto3 import client as boto3_client
from botocore.exceptions import ClientError
import docopt
from pymongo import (
    MongoClient,
    ConnectionFailure,
    OperationFailure,
    ServerSelectionTimeoutError,
)

# Local library
from fdi import __version__

SUCCEEDED_FOLDER = "success"
FAILED_FOLDER = "failed"


def import_data(
    s3_bucket=None,
    data_filename=None,
    db_hostname=None,
    db_port="27017",
    field_map=None,
    save_failed=True,
    save_succeeded=False,
    ssm_db_name=None,
    ssm_db_user=None,
    ssm_db_password=None,
    log_level="warning",
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

    field_map : str
        The S3 key for the JSON file containing a map of incoming field names
        to what they should be for the database.

    save_failed : bool
        Whether or not we should store unsuccessfully processed files

    save_succeeded : bool
        Whether or not we should store successfully processed files

    ssm_db_name : str
        The name of the parameter in AWS SSM that holds the name of the
        database to store the assessment data in.

    ssm_db_user : str
        The name of the parameter in AWS SSM that holds the database username
        with write permission to the assessment database.

    ssm_db_password : str
        The name of the parameter in AWS SSM that holds the database password
        for the user with write permission to the assessment database.

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
    ssm_client = boto3_client("ssm")

    # Securely create a temporary file to store the JSON data in
    temp_file_descriptor, temp_data_filepath = tempfile.mkstemp()

    logging.info(f"Retrieving {data_filename}...")
    try:
        # Fetch findings data file from S3 bucket
        s3_client.download_file(
            Bucket=s3_bucket, Key=data_filename, Filename=temp_data_filepath
        )
    except ClientError as err:
        logging.error("Error downloading file from S3 bucket")
        error_and_finish(
            err, False, s3_bucket, data_filename, temp_data_filepath, s3_client
        )

    try:
        # Fetch object for the field_map JSON
        field_map_object = s3_client.get_object(Bucket=s3_bucket, Key=field_map)
    except ClientError as err:
        logging.error("Error fetching object for the field_map with error")
        error_and_finish(
            err, False, s3_bucket, data_filename, temp_data_filepath, s3_client
        )

    try:
        # Load field_map JSONs
        field_map_dict = json.loads(
            field_map_object.get("Body", "{}").read().decode("utf-8")
        )
    except Exception as err:
        logging.error("Error loading JSON from field_map body with error")
        error_and_finish(
            err, False, s3_bucket, data_filename, temp_data_filepath, s3_client
        )

    logging.info(f"Configuration data loaded from {field_map}")
    logging.debug(f"Configuration data: {field_map_dict}")

    # Load data JSON
    try:
        with open(temp_data_filepath) as data_json_file:
            try:
                findings_data = json.load(data_json_file)
            except Exception as err:
                logging.error("Error loading JSON map from temp filepath")
                error_and_finish(
                    err, False, s3_bucket, data_filename, temp_data_filepath, s3_client
                )

            logging.info(f"JSON data loaded from {data_filename}.")

            # Fetch database credentials from AWS SSM
            db_info = dict()
            for ssm_param_name, key in (
                (ssm_db_name, "db_name"),
                (ssm_db_user, "username"),
                (ssm_db_password, "password"),
            ):
                try:
                    response = ssm_client.get_parameter(
                        Name=ssm_param_name, WithDecryption=True
                    )
                    db_info[key] = response["Parameter"]["Value"]
                except Exception as err:
                    logging.error("Error retrieving parameter from secure store")
                    error_and_finish(
                        err,
                        False,
                        s3_bucket,
                        data_filename,
                        temp_data_filepath,
                        s3_client,
                    )

            # Set up database connection
            credPw = urllib.parse.quote(db_info["password"])
            db_uri = (
                f"mongodb://{db_info['username']}:{credPw}@"
                f"{db_hostname}:{db_port}/{db_info['db_name']}"
            )

            # Connect to MongoDB with timeout so Lambda doesn't run over
            db_connection = MongoClient(
                host=db_uri, serverSelectionTimeoutMS=2500, tz_aware=True
            )
            db = db_connection[db_info["db_name"]]
            logging.info(
                f"DB connection set up to {db_hostname}:{db_port}/{db_info['db_name']}"
            )

            processed_findings = 0
            # Iterate through data and save each record to the database
            for finding in findings_data:
                # Replace or rename fields from replacement file
                for field in field_map_dict:
                    if field in finding.keys():
                        if field_map_dict[field]:
                            finding[field_map_dict[field]] = finding[field]
                        finding.pop(field, None)

                # Get RVA ID in format DDDD(.D+) from the end of the "RVA ID" field.
                rvaId = re.search(r"(\d{4})(?:\.\d+)?$", finding["RVA ID"])
                if rvaId:
                    finding["RVA ID"] = "RV" + rvaId.group(1)
                else:
                    logging.error(
                        f"Error extracting RVA ID from provided value '{finding['RVA ID']}'. Skipping record..."
                    )
                    continue
                # Only process appropriate findings records.
                if "RVA ID" in finding.keys() and "NCATS ID" in finding.keys():
                    # If the finding already exists, update it with new data.
                    # Otherwise insert it as a new document (upsert=True).
                    try:
                        db.findings.find_one_and_update(
                            {
                                "RVA ID": finding["RVA ID"],
                                "NCATS ID": finding["NCATS ID"],
                                "Severity": finding["Severity"],
                            },
                            {"$set": finding},
                            upsert=True,
                        )
                    except (
                        ConnectionFailure,
                        OperationFailure,
                        ServerSelectionTimeoutError,
                    ) as err:
                        logging.error("Error in(up)serting record to Mongo Database")
                        error_and_finish(
                            err,
                            True,
                            s3_bucket,
                            data_filename,
                            temp_data_filepath,
                            s3_client,
                        )

                    processed_findings += 1

            logging.info(
                f"{processed_findings}/{len(findings_data)} documents successfully processed"
            )

            if save_succeeded:
                # Create success folders depending on how processing went
                succeeded_filename = data_filename.replace(
                    ".json", f"_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')}.json"
                )
                key = f"{SUCCEEDED_FOLDER}/{succeeded_filename}"

                # Move data object to success directory
                try:
                    s3_client.copy_object(
                        Bucket=s3_bucket,
                        CopySource={"Bucket": s3_bucket, "Key": data_filename},
                        Key=key,
                    )
                    # Delete original object
                    s3_client.delete_object(Bucket=s3_bucket, Key=data_filename)
                except ClientError as err:
                    logging.error("Error organizing S3 bucket following processing.")
                    error_and_finish(
                        err,
                        False,
                        s3_bucket,
                        data_filename,
                        temp_data_filepath,
                        s3_client,
                    )

                logging.info(
                    f"Moved {data_filename} to the success directory under folder "
                    f"name {SUCCEEDED_FOLDER} as {succeeded_filename}"
                )
    except EnvironmentError as err:
        logging.error("Error opening temp file")
        error_and_finish(
            err, False, s3_bucket, data_filename, temp_data_filepath, s3_client
        )

    return True


def error_and_finish(
    err, save_failed, s3_bucket, data_filename, temp_data_filepath, s3_client
):
    """Handle errors that arise and organize error folder (if needed)."""
    logging.error(f"Error Message {type(err)}: {err}")

    if save_failed:
        # Create failure folders depending on how processing went
        failed_filename = data_filename.replace(
            ".json", f"_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')}.json"
        )
        key = f"{FAILED_FOLDER}/{failed_filename}"

        # Move data object to failure directory
        s3_client.copy_object(
            Bucket=s3_bucket,
            CopySource={"Bucket": s3_bucket, "Key": data_filename},
            Key=key,
        )
        try:
            s3_client.delete_object(Bucket=s3_bucket, Key=data_filename)
        except ClientError as delete_error:
            logging.error(f"Error deleting file with error: {delete_error}")

        logging.error(
            f"Error occurred. Moved {data_filename} to the failed directory"
            f" under folder name {FAILED_FOLDER} as {failed_filename}"
        )

    # Delete local temp data file(s) regardless of whether or not
    # any exceptions were thrown in the try block above
    os.remove(temp_data_filepath)
    logging.info(f"Deleted temporary {data_filename} from local filesystem")


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
        args["--field-map"],
        args["--save-failed"],
        args["--save-succeeded"],
        args["--ssm-db-name"],
        args["--ssm-db-user"],
        args["--ssm-db-password"],
        args["--log-level"],
    )

    # Stop logging and clean up
    logging.shutdown()

    return result


if __name__ == "__main__":
    main()
