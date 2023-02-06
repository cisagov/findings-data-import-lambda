"""Simple AWS Lambda handler to get environment variables and start the import.

This Lambda function expects the following environment variables to be
defined:
 1. s3_bucket - The AWS S3 bucket containing the data file.
 2. file_suffix - The suffix that a triggering key from the bucket above should
    have to be processed by this lambda.
 3. field_map - The AWS S3 object key in the above bucket that stores the JSON
    containing the rules for mapping fields of input data to the fields in the
    database.
 4. db_hostname - The hostname for the database that will store the processed
    data.
 5. db_port - The port that the database is listening on at the above hostname.
 6. save_failed - A boolean value specifying if inputs that error should be
    retained.
 7. save_succeeded - A boolean value specifying if inputs that succeed should be
    retained.
 8. ssm_db_name - The name of the parameter in AWS SSM Parameter Store that holds the
    name of the database that stored the processed data.
 9. ssm_db_user - The name of the parameter in AWS SSM Parameter Store that holds the
    database username with write permission to the above database.
10. ssm_db_password - The name of the parameter in AWS SSM Parameter Store that holds
    the password for the above database user.
"""

# Standard Python Libraries
import json
import logging
import os

# cisagov Libraries
from findings_data_import import import_data

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context) -> None:
    """Process the event and generate a response.

    The event should be an "ObjectCreated:Put" event with source S3 bucket and object
    key data matching the configuration for this lambda.

    :param event: The event dict that contains the parameters sent when the function
                  is invoked.
    :param context: The context in which the function is called.
    :return: The result of the action.
    """
    old_level = None

    # Update the logging level if necessary
    new_level = os.environ.get("log_level", "info").upper()
    if not isinstance(logging.getLevelName(new_level), int):
        logging.warning("Invalid log level %s passed. Using INFO instead.", new_level)
        new_level = "INFO"
    if logging.getLogger().getEffectiveLevel() != logging.getLevelName(new_level):
        old_level = logging.getLogger().getEffectiveLevel()
        logging.getLogger().setLevel(new_level)

    logging.debug("AWS Event was: %s", json.dumps(event))

    expected_event = "ObjectCreated:Put"

    # Get info in the S3 event notification message from
    # the parent Lambda function.
    record = event["Records"][0]

    object_key = record["s3"]["object"]["key"]

    # Retrieve environment variables
    s3_bucket = os.environ["s3_bucket"]
    object_suffix = os.environ["file_suffix"]
    field_map = os.environ["field_map"]
    save_failed = os.environ["save_failed"].lower() == "true"
    save_succeeded = os.environ["save_succeeded"].lower() == "true"

    database_host = os.environ["db_hostname"]
    database_port = os.environ["db_port"]

    ssm_db_name = os.environ["ssm_db_name"]
    ssm_db_username = os.environ["ssm_db_user"]
    ssm_db_password = os.environ["ssm_db_password"]

    # Verify event has correct eventName
    if record["eventName"] == expected_event:
        source_bucket = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]

        # Verify the source bucket and triggering object suffix
        if source_bucket == s3_bucket:
            if object_key.endswith(object_suffix):
                import_data(
                    s3_bucket=s3_bucket,
                    data_filename=object_key,
                    db_hostname=database_host,
                    db_port=database_port,
                    field_map=field_map,
                    save_failed=save_failed,
                    save_succeeded=save_succeeded,
                    ssm_db_name=ssm_db_name,
                    ssm_db_user=ssm_db_username,
                    ssm_db_password=ssm_db_password,
                )
            else:
                logging.warning(
                    'Object key "%s" does not end with required suffix "%s"',
                    object_key,
                    object_suffix,
                )
        else:
            logging.warning(
                'Expected "%s" event from S3 bucket "%s" but received event from S3 bucket "%s"',
                expected_event,
                s3_bucket,
                source_bucket,
            )
            logging.warning("Full AWS event: %s", json.dumps(event))
    else:
        logging.warning("Unexpected eventName received: %s", record["eventName"])
        logging.warning("Full AWS event: %s", json.dumps(event))

    if old_level is not None:
        logging.getLogger().setLevel(old_level)
