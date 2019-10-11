#!/usr/bin/env python3

"""findings_data_import: A tool for extracting, transforming and loading JSON data to Mongo.

The source data is a JSON file stored in an AWS S3 bucket.
The destination of the data is a Mongo database.

Usage:
  findings_data_import --s3-bucket=BUCKET --data-filename=FILE --db-hostname=HOST --success-folder=SUCCESS --error-folder=ERROR [--fields-filename=FILENAME] [--db-port=PORT] [--log-level=LEVEL] --ssm-db-name=DB --ssm-db-user=USER --ssm-db-password=PASSWORD
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
  --fields-filename=FILENAME  Filename storing replaced/removed fieldnames
  --success-folder=SUCCESS    The directory name used for storing successfully
                              processed files
  --error-folder=ERROR        The directory name used for storing unsuccessfully
                              processed files
  --log-level=LEVEL           If specified, then the log level will be set to
                              the specified value.  Valid values are "debug",
                              "info", "warning", "error", and "critical".
                              [default: warning]
  --ssm-db-name=DB            The name of the parameter in AWS SSM that holds
                              the name of the database to store the assessment
                              data in.
  --ssm-db-user=USER          The name of the parameter in AWS SSM that holds
                              the database username with write permission to
                              the assessment database.
  --ssm-db-password=PASSWORD  The name of the parameter in AWS SSM that holds
                              the database password for the user with write
                              permission to the assessment database.
"""

# Standard libraries
import copy
import datetime
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
from pymongo import MongoClient

# Local library
from fdi import __version__


def import_data(
    s3_bucket=None,
    data_filename=None,
    db_hostname=None,
    db_port="27017",
    fields_filename=None,
    log_level="warning",
    error_folder="error",
    success_folder="success",
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

    fields_filename : str
        The filename storing the field names that need to be removed or
        need their spelling changed

    starts_with : str
        The expression that will be validated against the start of the file

    contains : str
        The expression that will be validated against the contents of the file

    ends_with : str
        The expression that will be validated against the end of the file

    success_folder : str
        The success directory name that will be used to store successfully
        processed files

    error_folder : str
        The error directory name that will be used to store unsuccessfully
        processed files

    log_level : str
        If specified, then the log level will be set to the specified value.
        Valid values are "debug", "info", "warning", "error", and "critical".
        [default: warning]

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

    # Securely create a temporary file to store the fields to edit/remove in
    temp_field_file_descriptor, temp_field_data_filepath = tempfile.mkstemp()

    try:
        # Extract RV ID from filename
        logging.info(f"Extracting RVA ID from filename for {data_filename}")
        rvaId = data_filename.split("_")[0]

        logging.info(f"Retrieving {data_filename}...")

        # Fetch findings data file from S3 bucket
        s3_client.download_file(
            Bucket=s3_bucket, Key=data_filename, Filename=temp_data_filepath
        )

        # Fetch fields file from S3 bucket
        logging.info(f"Retrieving {fields_filename}...")
        s3_client.download_file(
            Bucket=s3_bucket, Key=fields_filename, Filename=temp_field_data_filepath
        )
        logging.info(f"Retrieved {data_filename} from S3 bucket {s3_bucket}")

        # Load data JSON
        with open(temp_data_filepath) as data_json_file, open(
            temp_field_data_filepath
        ) as fields_json_file:
            json_data = json.load(data_json_file)
            replacement_fields = json.load(fields_json_file)

        logging.info(f"JSON data loaded from {data_filename} and {fields_filename}")

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

        # Grab Unique Collection Field Names
        unique_collection_fields = set()
        records = db.testTable.find()
        for record in records:
            for field in record.keys():
                unique_collection_fields.add(field)

        # Iterate through data and save each record to the database
        for item in json_data:

            # Replace or rename fields from replacement file
            for field in replacement_fields:
                if field in item.keys():
                    if replacement_fields[field]:
                        item[replacement_fields[field]] = item[field]
                    item.pop(field, None)

            # Grab RVA ID from filename
            item["RVA ID"] = rvaId

            # Remove JSON objects that have fieldnames that don't exist in the collection
            valid = True
            for key in item.keys():
                if key not in unique_collection_fields:
                    logging.info(
                        f"Object fieldname {key} not recognized, skipping record: {item}..."
                    )
                    valid = False
                    break

            # Validate and Correct RV Number (if needed)
            # Skips record if RV number is invalid
            # Validation Rules:
            # - First two letters begin with 'RV'
            # - Ends with four or more digits
            #   * If more than four numbers are present,
            #     it will attempt to read the rest of the
            #     characters as numbers and remove
            #     unnecessary zeros. This will validate
            #     text values with multiple leading zeros.
            correctedRv = copy.deepcopy(item["RVA ID"])
            if correctedRv:
                isValid = re.search(r"RV\\d{4,0}", correctedRv)
                if isValid:
                    matchedRv = isValid.group()
                    matchedRvNumber = matchedRv.replace("RV", "")
                    if matchedRvNumber.isnumeric():
                        item["RVA ID"] = "{:04d}".format(int(matchedRvNumber))
                else:
                    rvaId = item["RVA ID"]
                    logging.warn(f"Invalid RV Number '{rvaId}' was found!")
                    raise Exception(f"Invalid RV Number '{rvaId}' was found!")

            # De-dup (RVA ID and NCATS ID and severity) - Skip duplicate records
            if (
                "RVA ID" in item.keys()
                and "NCATS ID" in item.keys()
                and "Severity" in item.keys()
            ):
                results = db.testTable.find_one(
                    {
                        "RVA ID": rvaId,
                        "NCATS ID": item["NCATS ID"],
                        "Severity": item["Severity"],
                    }
                )
                if results:
                    logging.warning("Duplicate record found.")
                    logging.warning(f"Full Record: {item}")
                    logging.warning("Skipping record...")

                if valid and not results:
                    db.testTable.insert_one(item)

        logging.info(f"{len(json_data)} documents " "successfully processed")

        # Create success folders depending on how processing went
        copySource = s3_bucket + "/" + data_filename
        key = f"{success_folder}/{data_filename.replace('.json', '')}_{str(datetime.datetime.now())}.json"

        # Move data object to success directory
        s3_client.copy_object(Bucket=s3_bucket, CopySource=copySource, Key=key)
        s3_client.delete_object(Bucket=s3_bucket, Key=data_filename)
        logging.info(
            f"Moved {data_filename} to the success directory under folder name {success_folder}"
        )
    except Exception as err:
        logging.warning(f"Error Message: {err}")

        # Create error folders depending on how processing went
        copySource = s3_bucket + "/" + data_filename
        key = f"{success_folder}/{data_filename.replace('.json', '')}_{str(datetime.datetime.now())}.json"

        # Move data object to error directory
        s3_client.copy_object(Bucket=s3_bucket, CopySource=copySource, Key=key)
        try:
            s3_client.delete_object(Bucket=s3_bucket, Key=data_filename)
        except ClientError as delete_error:
            logging.info(f"Error deleting file with error: {delete_error}")

        logging.info(
            f"Error occurred. Moved {data_filename} to the error directory under folder name {error_folder}"
        )
    finally:
        # Delete local temp data file(s) regardless of whether or not
        # any exceptions were thrown in the try block above
        os.remove(temp_data_filepath)
        os.remove(temp_field_data_filepath)
        logging.info(
            f"Deleted temporary {data_filename} and {temp_field_data_filepath} from local filesystem"
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
        args["--fields-filename"],
        args["--log-level"],
        args["--error-folder"],
        args["--success-folder"],
        args["--ssm-db-name"],
        args["--ssm-db-user"],
        args["--ssm-db-password"],
    )

    # Stop logging and clean up
    logging.shutdown()

    return result


if __name__ == "__main__":
    main()
