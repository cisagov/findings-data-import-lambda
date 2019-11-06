# findings-data-import-lambda ƛ #

[![Build Status](https://travis-ci.org/cisagov/findings-data-import-lambda.svg?branch=develop)](https://travis-ci.org/cisagov/findings-data-import-lambda.svg?branch=develop)
[![Total alerts](https://img.shields.io/lgtm/alerts/github/cisagov/findings-data-import-lambda)](https://img.shields.io/lgtm/alerts/github/cisagov/findings-data-import-lambda)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/github/cisagov/findings-data-import-lambda)](https://img.shields.io/lgtm/grade/python/github/cisagov/findings-data-import-lambda)

`findings-data-import-lambda` contains code to build an AWS Lambda function
that reads findings data from a JSON file in an S3 bucket and imports it
into a database.

## Example ##

Building the AWS Lambda zip file:

```console
cd ~/cisagov/findings-data-import-lambda
docker-compose down
docker-compose build
docker-compose up
```

## Field Mapping ##

The `--field-map` flag is leveraged to dynamically tell the script which fields
to remove and/or change. As the input JSON structure changes, the script is
capable of adapting to new or changing field name requirements. In the JSON
file it follows a key/value methodology, where the key is the original field
name (designated by the "field_to_replace" field in the example below) to find
in the input JSON and the value (designated by the "value_to_replace_field_with"
field in the example below) is the new field name desired. If the value is
blank, the script will remove that JSON element from the record.

### Example Field Map JSON File ###

```json
{
    "field_to_replace": "value_to_replace_field_with",
    "field_to_remove": ""
}
```

## Note ##

Please note that the corresponding Docker image _must_ be rebuilt
locally if the script `build.sh` changes.  Given that rebuilding the Docker
image is very fast (due to Docker's caching) if the script has not changed, it
is a very good idea to _always_ run the `docker-compose build` step when
using this tool.

## License ##

This project is in the worldwide [public domain](LICENSE).

This project is in the public domain within the United States, and
copyright and related rights in the work worldwide are waived through
the [CC0 1.0 Universal public domain
dedication](https://creativecommons.org/publicdomain/zero/1.0/).

All contributions to this project will be released under the CC0
dedication. By submitting a pull request, you are agreeing to comply
with this waiver of copyright interest.
