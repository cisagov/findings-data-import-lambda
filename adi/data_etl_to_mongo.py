#!/usr/bin/env python3

"""data-etl-to-mongo: A tool for extracting, transforming
and loading JSON data to Mongo.

The source data is a JSON file stored in an AWS S3 bucket.
The destination of the data is a Mongo database.

Usage:
  data-etl-to-mongo --s3-bucket=BUCKET --data-filename=FILE --db-hostname=HOST --starts-with=STARTS --contains=CONTAINS --ends-with=ENDS --success-folder=SUCCESS --error-folder=ERROR [--fields-filename=FILENAME] [--db-port=PORT] [--log-level=LEVEL]
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
  --fields-filename=FILENAME  Filename storing replaced/removed fieldnames
  --starts-with=STARTS        Expression used for validating the beginning of
                              the assessment data filename
  --contains=CONTAINS         Expression used for validating the contents of
                              the assessment data filename
  --ends-with=ENDS            Expression used for validating the end of
                              the assessment data filename
  --success-folder=SUCCESS    The directory name used for storing successfully
                              processed files
  --error-folder=ERROR        The directory name used for storing unsuccessfully
                              processed files                              
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
    fields_filename=None,
    log_level="warning",
    error_folder="error",
    success_folder="success"
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

    Returns
    -------
    bool : Returns a boolean indicating if the data import was
    successful.

    """
    # Boto3 clients for S3 and SSM
    s3_client = boto3_client("s3")

    # Securely create a temporary file to store the JSON data in
    temp_file_descriptor, temp_data_filepath = tempfile.mkstemp()
    temp_field_file_descriptor, temp_field_data_filepath = tempfile.mkstemp()

    try:
        logging.info(f'Extracting RVA ID from filename for {data_filename}')
        rvaId = data_filename.split('_')[0]

        logging.info(f'Retrieving {data_filename}...')
        # Fetch data file from S3 bucket
        s3_client.download_file(
            Bucket=s3_bucket, Key=data_filename, Filename=f"{temp_data_filepath}"
        )

        logging.info(f'Retrieving {fields_filename}...')
        s3_client.download_file(
            Bucket=s3_bucket, Key=fields_filename, Filename=f"{temp_field_data_filepath}"
        )
        logging.info(f"Retrieved {data_filename} from S3 bucket {s3_bucket}")

        # Load data JSON
        with open(f"{temp_data_filepath}") as data_json_file, open(f'{temp_field_data_filepath}') as fields_json_file:
            json_data = json.load(data_json_file)
            replacement_fields = json.load(fields_json_file)

            # Filename RVID_ - use text up to first underscore as RV ID

        logging.info(f"JSON data loaded from {data_filename} and {fields_filename}")

        db_connection = MongoClient(host=f'mongodb://{db_hostname}:27017', serverSelectionTimeoutMS=2500, tz_aware=True)
        db = db_connection['test-db']
        logging.info(
            f"DB connection set up to {db_hostname}:{db_port}/test-db"
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
            item['RVA ID'] = rvaId

            # Remove un-matched JSON objects
            valid = True
            for key in item.keys():
                if key not in unique_collection_fields:
                    logging.info(f'Object fieldname {key} not recognized, skipping record: {item}...')
                    valid = False
                    break

            # De-dup (RVA ID and NCATS ID and severity) are unique
            if "RVA ID" in item.keys() and "NCATS ID" in item.keys() and "Severity" in item.keys():
                results = db.testTable.find_one({"RVA ID": rvaId, "NCATS ID": item["NCATS ID"], "Severity": item["Severity"]})
                if results:
                    logging.warning('Duplicate record found.')
                    logging.warning(f'Full Record: {item}')
                    logging.warning('Skipping record...')

                if valid and not results:
                    db.testTable.insert_one(item)

        logging.info(
            f"{len(json_data)} documents "
            "successfully inserted in database"
        )

        copySource = s3_bucket + '/' + data_filename
        key = success_folder + '/' + data_filename.replace('.json', '') + '_' + str(datetime.datetime.now()) + '.json'

        # Move data object to success directory
        s3_client.copy_object(Bucket=s3_bucket, CopySource=copySource, Key=key)
        s3_client.delete_object(Bucket=s3_bucket, Key=data_filename)
        logging.info(f"Moved {data_filename} to the success directory under folder name {success_folder}")
    except Exception as err:
        logging.warning(f'Error Message: {err}')

        copySource = s3_bucket + '/' + data_filename
        key = error_folder + '/' + data_filename.replace('.json', '') + '_' + str(datetime.datetime.now()) + '.json'

        # Move data object to error directory
        s3_client.copy_object(Bucket=s3_bucket, CopySource=copySource, Key=key)
        try:
            s3_client.delete_object(Bucket=s3_bucket, Key=data_filename)
        except:
            logging.info('Error deleting file')

        logging.info(f"Error occurred. Moved {data_filename} to the error directory under folder name {error_folder}")
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
        args["--fields-filename"],
        args["--log-level"],
        args["--error-folder"],
        args["--success-folder"]
    )

    # Stop logging and clean up
    logging.shutdown()

    return result


if __name__ == "__main__":
    main()
