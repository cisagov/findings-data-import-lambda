# findings-data-import-lambda ƛ #

[![Build Status](https://travis-ci.org/mzack5020/findings-data-import.svg?branch=master)](https://travis-ci.org/mzack5020/findings-data-import.svg?branch=master)
[![Total alerts](https://img.shields.io/lgtm/alerts/github/mzack5020/findings-data-import)](https://img.shields.io/lgtm/alerts/github/mzack5020/findings-data-import)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/github/mzack5020/findings-data-import)](https://img.shields.io/lgtm/grade/python/github/mzack5020/findings-data-import)

`findings-data-import-lambda` contains code to build an AWS Lambda function
that reads findings data from a JSON file in an S3 bucket and imports it
into a database.

## Example ##

Building the AWS Lambda zip file:

1. `cd ~/cisagov/findings-data-import-lambda`
1. `docker-compose down`
1. `docker-compose build`
1. `docker-compose up`

## Fields to Replace ##

The fields_to_replace.json file is leveraged to dynamically tell the script
which fields to remove and/or change. As the input JSON structure changes, the
script is capable of adapting to new or changing field name requirements. In
the JSON file, it follows a key/value methodology, where the key is the
original field name to find in the input JSON and the value is the new field
name desired. If the value is blank, the script will remove that JSON element
from the record.

## Note ##

Please note that the corresponding Docker image _must_ be rebuilt
locally if the script `build.sh` changes.  Given that rebuilding the Docker
image is very fast (due to Docker's caching) if the script has not changed, it
is a very good idea to _always_ run the `docker-compose build` step when
using this tool.

## License ##

This project is in the worldwide [public domain](LICENSE.md).

This project is in the public domain within the United States, and
copyright and related rights in the work worldwide are waived through
the [CC0 1.0 Universal public domain
dedication](https://creativecommons.org/publicdomain/zero/1.0/).

All contributions to this project will be released under the CC0
dedication. By submitting a pull request, you are agreeing to comply
with this waiver of copyright interest.
