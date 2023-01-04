# findings-data-import-lambda ƛ

[![GitHub Build Status](https://github.com/cisagov/findings-data-import-lambda/workflows/build/badge.svg)](https://github.com/cisagov/findings-data-import-lambda/actions)

`findings-data-import-lambda` contains code to build an AWS Lambda function
that reads findings data from a JSON file in an S3 bucket and imports it
into a database.

## Field mapping

The Lambda supports using a field map configuration JSON in S3 (object key
provided to the Lambda as the `field_map` environment variable) to dynamically
tell the script which fields to remove and/or change. As the input JSON
structure changes the script is capable of adapting to new or changing field
name requirements. The JSON file is a simple dictionary, where each key is the
original field name (designated by `"field_to_replace"` in the example below)
to find in the input JSON and the value (designated by `"value_to_replace_field_with"`
in the example below) is the new field name desired. If the value is empty the
script will remove that element from the record.

### Example field map JSON file

```json
{
  "field_to_replace": "value_to_replace_field_with",
  "field_to_remove": ""
}
```

## Operational note

This script will attempt to extract the RVA ID from the `RVA ID` field in a record
after field mapping has taken place. It expects the ID found to end in the format
`DDDD`, but allows an increment such that `0123.4` is valid. However, the matched
ID is reduced to the four leading digits in this case.

## Building the base Lambda image

The base Lambda image can be built with the following command:

```console
docker compose build
```

This base image is used both to build a deployment package and to run the
Lambda locally.

## Building a deployment package

You can build a deployment zip file to use when creating a new AWS Lambda
function with the following command:

```console
docker compose up build_deployment_package
```

This will output the deployment zip file in the root directory.

## Running the Lambda locally

The configuration in this repository allows you run the Lambda locally for
testing as long as you do not need explicit permissions for other AWS
services. This can be done with the following command:

```console
docker compose up --detach run_lambda_locally
```

You can then invoke the Lambda using the following:

```console
 curl -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

The `{}` in the command is the invocation event payload to send to the Lambda
and would be the value given as the `event` argument to the handler.

Once you are finished you can stop the detached container with the following command:

```console
docker compose down
```

## How to update Python dependencies

The Python dependencies are maintained using a [Pipenv](https://github.com/pypa/pipenv)
configuration for each supported Python version. Changes to requirements
should be made to the respective `src/py<Python version>/Pipfile`. More
information about the `Pipfile` format can be found [here](https://pipenv.pypa.io/en/latest/basics/#example-pipfile-pipfile-lock).
The accompanying `Pipfile.lock` files contain the specific dependency versions
that will be installed. These files can be updated like so (using the Python
3.9 configuration as an example):

```console
cd src/py3.9
pipenv lock
```

## Contributing

We welcome contributions! Please see [`CONTRIBUTING.md`](CONTRIBUTING.md) for
details.

## License

This project is in the worldwide [public domain](LICENSE).

This project is in the public domain within the United States, and
copyright and related rights in the work worldwide are waived through
the [CC0 1.0 Universal public domain
dedication](https://creativecommons.org/publicdomain/zero/1.0/).

All contributions to this project will be released under the CC0
dedication. By submitting a pull request, you are agreeing to comply
with this waiver of copyright interest.
