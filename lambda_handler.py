"""This module contains the lamdba_handler code."""

import logging
import os

# Local module
from fdi import findings_data_import as fdi

# This Lambda function expects the following environment variables to be
# defined:
# 1. s3_bucket - The AWS S3 bucket containing the data file
# 2. data_filename - The name of the file containing the data in
# the S3 bucket above
# 3. db_hostname - The hostname that has the database to store the
# data in
# 4. db_port - The port that the database server is listening on
# 5. fields_filename - Filename in S3 where the replacement/removed fieldnames
# are stored. This file should map old fieldname spellings to new fieldname
# spellings. (i.e. {"old_value": "new_value"})
# 6. starts_with - The expression that should located at the beginning of
# the filename.
# 7. contains - The expression that should be located somewhere within the
# filename.
# 8. ends_with - The expression that should be located at the end of the
# filename.
# 9. success_folder - The directory name of the folder where successfully
# processed files will be located after the script runs.
# 10. error_folder - The directory name of the folder where unsuccessfully
# processed files will be located after the script runs.

# In the case of AWS Lambda, the root logger is used BEFORE our Lambda handler
# runs, and this creates a default handler that goes to the console.  Once
# logging has been configured, calling logging.basicConfig() has no effect.  We
# can get around this by removing any root handlers (if present) before calling
# logging.basicConfig().  This unconfigures logging and allows --debug to
# affect the logging level that appears in the CloudWatch logs.
#
# See
# https://stackoverflow.com/questions/1943747/python-logging-before-you-run-logging-basicconfig
# and
# https://stackoverflow.com/questions/37703609/using-python-logging-with-aws-lambda
# for more details.
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

        # Set up logging
        log_level = logging.INFO
        logging.basicConfig(
            format="%(asctime)-15s %(levelname)s %(message)s", level=log_level
        )


def handler(event, context):
    """Handle all Lambda events."""
    logging.debug("AWS Event was: {}".format(event))

    # Get info in the S3 event notification message from
    # the parent Lambda function.
    record = event["Records"][0]

    filename = record["s3"]["object"]["key"]

    # Verify event has correct eventName
    if record["eventName"] == "ObjectCreated:Put":
        # Verify event originated from correct bucket and key
        # Uses environment variables for filename validation
        if record["s3"]["bucket"]["name"] == os.environ["s3_bucket"] and (
            os.environ["contains"] in filename
            and filename.startswith(os.environ["starts_with"])
            and filename.endswith(os.environ["ends_with"])
        ):
            # Import the data
            fdi.import_data(
                s3_bucket=os.environ["s3_bucket"],
                data_filename=filename,
                db_hostname=os.environ["db_hostname"],
                db_port=os.environ["db_port"],
                fields_filename=os.environ["fields_filename"],
                log_level=log_level,
                error_folder=os.environ["error_folder"],
                success_folder=os.environ["success_folder"],
            )
        elif not filename.startswith(os.environ["starts_with"]):
            logging.warning(
                f"Filename {filename} failed validation, did not start with {os.environ['starts_with']}"
            )
        elif os.environ["contains"] not in filename:
            logging.warning(
                f"Filename {filename} failed validation, did not contain {os.environ['contains']}"
            )
        elif not filename.endswith(os.environ["ends_with"]):
            logging.warning(
                f"Filename {filename} failed validation, did not end with {os.environ['ends_with']}"
            )
        else:
            logging.warning(
                "Expected ObjectCreated event from S3 bucket "
                f"{os.environ['s3_bucket']} "
                f"with key {os.environ['data_filename']}, but "
                "received event from S3 bucket "
                f"{record['s3']['bucket']['name']} with key "
                f"{record['s3']['object']['key']}"
            )
            logging.warning("Full AWS event: {}".format(event))
    else:
        logging.warning("Unexpected eventName received: {}".format(record["eventName"]))
        logging.warning("Full AWS event: {}".format(event))
