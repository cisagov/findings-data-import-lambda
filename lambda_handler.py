"""This module contains the lamdba_handler code."""

import logging
import os

# Local module
from fdi import findings_data_import as fdi

# This Lambda function expects the following environment variables to be
# defined:
# 1. s3_bucket - The AWS S3 bucket containing the data file
# 2. data_filename - The key of the file containing the data in
#    the S3 bucket above
# 3. db_hostname - The hostname that has the database to store the
#    data in
# 4. db_port - The port that the database server is listening on
# 5. save_error - A boolean value specifying if inputs that error should be
#    retained.
# 6. save_success - A boolean value specifying if inputs that succeed should be
#    retained.
# 7. ssm_db_name - The name of the parameter in AWS SSM that holds the name
#    of the database to store the assessment data in.
# 8. ssm_db_user - The name of the parameter in AWS SSM that holds the
#    database username with write permission to the assessment database.
# 9. ssm_db_password - The name of the parameter in AWS SSM that holds the
#    database password for the user with write permission to the assessment
#    database.

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
    logging.debug(f"AWS Event was: {event}")

    # Get info in the S3 event notification message from
    # the parent Lambda function.
    record = event["Records"][0]

    filename = record["s3"]["object"]["key"]

    # Verify event has correct eventName
    if record["eventName"] == "ObjectCreated:Put":
        # Verify event originated from correct bucket and key
        if record["s3"]["bucket"]["name"] == os.environ["s3_bucket"] and record["s3"][
            "object"
        ]["key"].endswith(os.environ["file_suffix"]):
            # Import the data
            fdi.import_data(
                s3_bucket=os.environ["s3_bucket"],
                data_filename=record["s3"]["object"]["key"],
                db_hostname=os.environ["db_hostname"],
                db_port=os.environ["db_port"],
                valid_fields=os.environ["valid_fields"],
                field_map=os.environ["field_map"],
                save_failed=os.environ["save_failed"],
                save_succeeded=os.environ["save_succeeded"],
                ssm_db_name=os.environ["ssm_db_name"],
                ssm_db_user=os.environ["ssm_db_user"],
                ssm_db_password=os.environ["ssm_db_password"],
                log_level=log_level,
            )
        elif not filename.endswith(os.environ["file_suffix"]):
            logging.warning(
                f'Filename "{filename}" failed validation, '
                f"did not end with {os.environ['file_suffix']}"
            )
        else:
            logging.warning(
                "Expected ObjectCreated:Put event from S3 bucket "
                f"{os.environ['s3_bucket']} "
                f"with key {os.environ['data_filename']}, but "
                "received event from S3 bucket "
                f"{record['s3']['bucket']['name']} with key "
                f"{record['s3']['object']['key']}"
            )
            logging.warning(f"Full AWS event: {event}")
    else:
        logging.warning(f"Unexpected eventName received: {record['eventName']}")
        logging.warning(f"Full AWS event: {event}")
