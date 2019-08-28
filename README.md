# findings-data-import-lambda ƛ #

<!-- [![Build Status](https://travis-ci.com/cisagov/findings-data-import-lambda.svg?branch=develop)](https://travis-ci.com/cisagov/findings-data-import-lambda) -->
<!-- [![Total alerts](https://img.shields.io/lgtm/alerts/g/cisagov/findings-data-import-lambda.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/cisagov/findings-data-import-lambda/alerts/) -->
<!-- [![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/cisagov/findings-data-import-lambda.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/cisagov/findings-data-import-lambda/context:python) -->

`findings-data-import-lambda` contains code to build an AWS Lambda function
that reads findings data from a JSON file in an S3 bucket and imports it
into a database.

## Example ##

Building the AWS Lambda zip file:

1. `cd ~/cisagov/findings-data-import-lambda`
1. `docker-compose down`
1. `docker-compose build`
1. `docker-compose up`

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
