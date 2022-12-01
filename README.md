# findings-data-import-lambda ƛ #

<<<<<<< HEAD
[![GitHub Build Status](https://github.com/cisagov/findings-data-import-lambda/workflows/build/badge.svg)](https://github.com/cisagov/findings-data-import-lambda/actions)
[![Coverage Status](https://coveralls.io/repos/github/cisagov/findings-data-import-lambda/badge.svg?branch=develop)](https://coveralls.io/github/cisagov/findings-data-import-lambda?branch=develop)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/cisagov/findings-data-import-lambda.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/cisagov/findings-data-import-lambda/alerts/)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/cisagov/findings-data-import-lambda.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/cisagov/findings-data-import-lambda/context:python)
[![Known Vulnerabilities](https://snyk.io/test/github/cisagov/findings-data-import-lambda/develop/badge.svg)](https://snyk.io/test/github/cisagov/findings-data-import-lambda)
=======
## ⚠ Notice ⚠ ##

This project has been deprecated. A replacement project using a more modern
approach can be found at [cisagov/skeleton-aws-lambda-python](https://github.com/cisagov/skeleton-aws-lambda-python).
If you need to create AWS Lambdas using Python runtimes please base your project
on that skeleton.

[![GitHub Build Status](https://github.com/cisagov/skeleton-aws-lambda/workflows/build/badge.svg)](https://github.com/cisagov/skeleton-aws-lambda/actions)
[![Coverage Status](https://coveralls.io/repos/github/cisagov/skeleton-aws-lambda/badge.svg?branch=develop)](https://coveralls.io/github/cisagov/skeleton-aws-lambda?branch=develop)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/cisagov/skeleton-aws-lambda.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/cisagov/skeleton-aws-lambda/alerts/)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/cisagov/skeleton-aws-lambda.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/cisagov/skeleton-aws-lambda/context:python)
[![Known Vulnerabilities](https://snyk.io/test/github/cisagov/skeleton-aws-lambda/develop/badge.svg)](https://snyk.io/test/github/cisagov/skeleton-aws-lambda)
>>>>>>> d2561c411f66c6477d7a3932860d98bc519b31e8

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

## Operational Note ##

This script will attempt to extract the RVA ID from the `RVA ID` field in a record
after field mapping has taken place. It expects the ID found to end in the format
`DDDD`, but allows an increment such that `0123.4` is valid. However, the matched
ID is reduced to the four leading digits in this case.

## Docker Note ##

Please note that the corresponding Docker image _must_ be rebuilt
locally if the script `build.sh` changes.  Given that rebuilding the Docker
image is very fast (due to Docker's caching) if the script has not changed, it
is a very good idea to _always_ run the `docker-compose build` step when
using this tool.

## Contributing ##

We welcome contributions!  Please see [`CONTRIBUTING.md`](CONTRIBUTING.md) for
details.

## License ##

This project is in the worldwide [public domain](LICENSE).

This project is in the public domain within the United States, and
copyright and related rights in the work worldwide are waived through
the [CC0 1.0 Universal public domain
dedication](https://creativecommons.org/publicdomain/zero/1.0/).

All contributions to this project will be released under the CC0
dedication. By submitting a pull request, you are agreeing to comply
with this waiver of copyright interest.
