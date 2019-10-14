#!/usr/bin/env python3

"""findings_data_import: A tool for extracting, transforming and loading JSON data to Mongo.

The source data is a JSON file stored in an AWS S3 bucket.
The destination of the data is a Mongo database.

Usage:
  findings_data_import --s3-bucket=BUCKET --data-filename=FILE --db-hostname=HOST --valid-fields=VALID --field-map=MAP --ssm-db-name=DB --ssm-db-user=USER --ssm-db-password=PASSWORD [--save-failed=FAILED] [--save-succeeded=SUCCEEDED] [--db-port=PORT] [--log-level=LEVEL]
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
  --valid-fields=VALID        The S3 key for the JSON file containing a list of
                              valid fields for a findings document.
  --field-map=MAP             The S3 key for the JSON file containing a map of
                              incoming field names to what they should be for
                              the database.
  --save-failed=FAILED        The directory name used for storing unsuccessfully
                              processed files. [default: True]
  --save-succesded=SUCCEEDED  The directory name used for storing successfully
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
import datetime
import json
import logging
import os
import tempfile
import urllib

# Third-party libraries (install with pip)
from boto3 import client as boto3_client
from botocore.exceptions import ClientError
import docopt
from pymongo import MongoClient

# Local library
from fdi import __version__

SUCCEEDED_FOLDER = "success"
FAILED_FOLDER = "failed"


def import_data(
    s3_bucket=None,
    data_filename=None,
    db_hostname=None,
    db_port="27017",
    valid_fields=None,
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

    valid_fields : str
        The S3 key for the JSON file containing a list of valid fields for a
        findings document.

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

    try:
        # Extract RV ID from filename
        logging.info(f"Extracting RVA ID from filename for {data_filename}")
        rvaId = data_filename.split("_")[0]

        logging.info(f"Retrieving {data_filename}...")

        # Fetch findings data file from S3 bucket
        s3_client.download_file(
            Bucket=s3_bucket, Key=data_filename, Filename=temp_data_filepath
        )

        # Fetch object for the valid_fields JSON
        valid_fields_object = s3_client.get_object(Bucket=s3_bucket, Key=valid_fields)

        # Fetch object for the field_map JSON
        field_map_object = s3_client.get_object(Bucket=s3_bucket, Key=field_map)

        # Load valid_fields and field_map JSONs
        valid_fields_list = json.loads(valid_fields_object.get("Body", ()))
        field_map_dict = json.loads(field_map_object.get("Body", {}))

        logging.info(
            f"Configuration data loaded from {valid_fields}" f" and {field_map}"
        )

        # Load data JSON
        with open(temp_data_filepath) as data_json_file:
            findings_data = json.load(data_json_file)

        logging.info(f"JSON data loaded from {data_filename}.")

        # Fetch database credentials from AWS SSM
        db_info = dict()
        for ssm_param_name, key in (
            (ssm_db_name, "db_name"),
            (ssm_db_user, "username"),
            (ssm_db_password, "password"),
        ):
            response = ssm_client.get_parameter(
                Name=ssm_param_name, WithDecryption=True
            )
            db_info[key] = response["Parameter"]["Value"]

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
            f"DB connection set up to {db_hostname}:{db_port}/" f"{db_info['db_name']}"
        )

        # Iterate through data and save each record to the database
        for finding in findings_data:
            # Replace or rename fields from replacement file
            for field in field_map_dict:
                if field in finding.keys():
                    if field_map_dict[field]:
                        finding[field_map_dict[field]] = finding[field]
                    finding.pop(field, None)

            # Grab RVA ID from filename
            finding["RVA ID"] = rvaId

            # Remove JSON objects that have fieldnames that don't exist in the collection
            valid = True
            for key in finding.keys():
                if key not in valid_fields_list:
                    logging.info(
                        f"Object fieldname {key} not recognized, skipping record: {finding}..."
                    )
                    valid = False
                    break

            # De-dupe (RVA ID and NCATS ID and severity) - Skip duplicate records
            if (
                "RVA ID" in finding.keys()
                and "NCATS ID" in finding.keys()
                and "Severity" in finding.keys()
            ):
                results = db.findings.find_one(
                    {
                        "RVA ID": rvaId,
                        "NCATS ID": finding["NCATS ID"],
                        "Severity": finding["Severity"],
                    }
                )

            if results:
                logging.warning("Duplicate record found.")
                logging.warning(f"Full Record: {finding}")
                logging.warning("Skipping record...")

            if valid and not results:
                db.findings.insert_one(finding)

        logging.info(f"{len(findings_data)} documents successfully processed")

        if save_succeeded:
            # Create success folders depending on how processing went
            succeeded_filename = data_filename.replace(
                ".json", f"_{str(datetime.datetime.now())}.json"
            )
            key = f"{SUCCEEDED_FOLDER}/{succeeded_filename}"

            # Move data object to success directory
            s3_client.copy_object(
                Bucket=s3_bucket,
                CopySource={"Bucket": s3_bucket, "Key": data_filename},
                Key=key,
            )
            # Delete original object
            s3_client.delete_object(Bucket=s3_bucket, Key=data_filename)

            logging.info(
                f"Moved {data_filename} to the success directory under folder "
                f"name {SUCCEEDED_FOLDER}"
            )
    except Exception as err:
        logging.warning(f"Error Message: {err}")

        if save_failed:
            # Create failure folders depending on how processing went
            failed_filename = data_filename.replace(
                ".json", f"_{str(datetime.datetime.now())}.json"
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
                logging.info(f"Error deleting file with error: {delete_error}")

            logging.info(
                f"Error occurred. Moved {data_filename} to the failed directory"
                f" under folder name {FAILED_FOLDER}"
            )
    finally:
        # Delete local temp data file(s) regardless of whether or not
        # any exceptions were thrown in the try block above
        os.remove(temp_data_filepath)
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
        args["--valid-fields"],
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
