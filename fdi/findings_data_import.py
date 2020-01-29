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

# Standard Python Libraries
from datetime import datetime
import json
import logging
import os
import re
import tempfile
import urllib.parse

# Third-Party Libraries
from boto3 import client as boto3_client
from botocore.exceptions import ClientError
import docopt
from pymongo import MongoClient

# cisagov Libraries
from fdi import __version__

SUCCEEDED_FOLDER = "success"
FAILED_FOLDER = "failed"


def move_processed_file(s3_client, bucket, folder, filename):
    """Copy a processed file to the appropriate directory and delete the original."""
    new_filename = filename.replace(
        ".json", f"_{datetime.now().strftime('%Y-%m-%d_%H:%M:%S.%f')}.json"
    )
    key = f"{folder}/{new_filename}"

    try:
        # Copy object to appropriate directory in the S3 bucket.
        s3_client.copy_object(
            Bucket=bucket, CopySource={"Bucket": bucket, "Key": filename}, Key=key
        )
        logging.info(
            f"Successfully copied '{filename}' to directory '{folder}' as '{new_filename}'."
        )

        # Delete the original object.
        s3_client.delete_object(Bucket=bucket, Key=filename)
        logging.info(f"Successfully deleted original '{filename}'.")
    except ClientError as delete_error:
        logging.error(
            f"Failed while moving '{filename}' to '{folder}' directory."
            f"Error: {delete_error}"
        )


def skip_record(index, file, message):
    """Print a standard message when a record must be skipped."""
    logging.warning(f"Skipping record {index} of '{file}': {message}.")


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
        # This allows us to access keys with spaces in them. When they are passed
        # in to the lambda the psaces are replaced with plus signs which results
        # in being unable to access the object with the given key. This WILL
        # result in issues if the object also has plus signs as part of its name
        # and ideally object names should contain none of the characters listed
        # in https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
        # under the section "Characters That Might Require Special Handling".
        data_filename = urllib.parse.unquote_plus(data_filename)
        logging.info(f"Retrieving {data_filename}...")

        # Fetch findings data file from S3 bucket
        s3_client.download_file(
            Bucket=s3_bucket, Key=data_filename, Filename=temp_data_filepath
        )

        # Fetch object for the field_map JSON
        field_map_object = s3_client.get_object(Bucket=s3_bucket, Key=field_map)

        # Load field_map JSONs
        field_map_dict = json.loads(
            field_map_object.get("Body", "{}").read().decode("utf-8")
        )
        logging.info(f"Configuration data loaded from {field_map}")
        logging.debug(f"Configuration data: {field_map_dict}")

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
            f"DB connection set up to {db_hostname}:{db_port}/{db_info['db_name']}"
        )

        processed_findings = 0
        # Iterate through data and save each record to the database
        for index, finding in enumerate(findings_data):
            # Replace or rename fields from replacement JSON
            for field in field_map_dict:
                if field in finding.keys():
                    if field_map_dict[field]:
                        finding[field_map_dict[field]] = finding[field]
                    finding.pop(field, None)

            # Get RVA ID in format DDDD([.-]D+) from the end of the "RVA ID" field.
            rvaId = re.search(r"(\d{4})(?:[.-](\d+))?$", finding["RVA ID"])
            if rvaId:
                rID = f"RV{rvaId.group(1)}"
                if rvaId.group(2) is not None:
                    rID += f".{rvaId.group(2)}"
                finding["RVA ID"] = rID
            else:
                skip_record(
                    index,
                    data_filename,
                    f"Unable to extract valid RVA ID from '{finding['RVA ID']}'",
                )
                continue
            # Only process appropriate findings records.
            if "RVA ID" in finding.keys() and "NCATS ID" in finding.keys():
                # If the finding already exists, update it with new data.
                # Otherwise insert it as a new document (upsert=True).
                db.findings.find_one_and_update(
                    {
                        "RVA ID": finding["RVA ID"],
                        "NCATS ID": finding["NCATS ID"],
                        "Severity": finding["Severity"],
                    },
                    {"$set": finding},
                    upsert=True,
                )

                processed_findings += 1
            else:
                skip_record(
                    index, data_filename, "Missing 'RVA ID' or 'NCATS ID' field"
                )

        logging.info(
            f"{processed_findings}/{len(findings_data)} documents successfully processed from '{data_filename}'."
        )

        if save_succeeded:
            move_processed_file(s3_client, s3_bucket, SUCCEEDED_FOLDER, data_filename)
    except Exception as err:
        logging.error(f"Error Message {type(err)}: {err}")

        if save_failed:
            move_processed_file(s3_client, s3_bucket, FAILED_FOLDER, data_filename)
    finally:
        # Delete local temp data file(s) regardless of whether or not
        # any exceptions were thrown in the try block above
        os.remove(temp_data_filepath)
        logging.info(
            f"Deleted working copy of '{data_filename}' from local filesystem."
        )

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
        args["--field-map"],
        args["--save-failed"],
        args["--save-succeeded"],
        args["--ssm-db-name"],
        args["--ssm-db-user"],
        args["--ssm-db-password"],
    )

    # Stop logging and clean up
    logging.shutdown()

    return result


if __name__ == "__main__":
    main()
