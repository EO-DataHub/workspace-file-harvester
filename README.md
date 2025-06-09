# Workspace File Harvester

Collects user-uploaded files from S3. STAC files are passed to the transformer and ingestor for harvesting and access 
policy files are used to update public access of files, folders and workflows. 

It is designed to operate as part of a data pipeline which is triggered by the POST endpoint. Files are collected 
from an S3 bucket and are sent to a transformer if STAC files, or used to update access permissions if not. After 
harvesting, it sends messages back to Pulsar to notify downstream services of the new or updated catalogue entries.


## Features

- **File harvest** - checks for new files in S3 bucket and sends STAC files into the harvest pipeline
- **Permission updates** - data in access files is used to update access permissions
- **Viewing Logs** - ability to view logs relating to the harvest of a particular workspace


## Getting started

### Install via makefile

```commandline
make setup
```

This will create a virtual environment called `venv`, build `requirements.txt` and
`requirements-dev.txt` from `pyproject.toml` if they're out of date, install the Python
and Node dependencies and install `pre-commit`.

It's safe and fast to run `make setup` repeatedly as it will only update these things if
they have changed.

After `make setup` you can run `pre-commit` to run pre-commit checks on staged changes and
`pre-commit run --all-files` to run them on all files. This replicates the linter checks that
run from GitHub actions.


### Alternative installation

You will need Python 3.12. On Debian you may need:
* `sudo add-apt-repository -y 'deb http://ppa.launchpad.net/deadsnakes/ppa/ubuntu focal main'` (or `jammy` in place of `focal` for later Debian)
* `sudo apt update`
* `sudo apt install python3.12 python3.12-venv`

and on Ubuntu you may need
* `sudo add-apt-repository -y 'ppa:deadsnakes/ppa'`
* `sudo apt update`
* `sudo apt install python3.11 python3.11-venv`

To prepare running it:

* `virtualenv venv -p python3.12`
* `. venv/bin/activate`
* `rehash`
* `python -m ensurepip -U`
* `pip3 install -r requirements.txt`
* `pip3 install -r requirements-dev.txt`

You should also configure your IDE to use black so that code is automatically reformatted on save.


## Configuration

The file harvester is configured through environment variables and query parameters


### Environment variables

The following environment variables are required:
- `ELASTICSEARCH_URL` - URL for elasticsearch (string)
- `API_KEY` - elasticsearch API key (string)
- `ROOT_PATH` - root path for workspaces
- `SOURCE_S3_BUCKET` - bucket where files are uploaded to
- `TARGET_S3_BUCKET` - bucket containing files for harvest
- `BLOCK_OBJECT_STORE_DATA_ACCESS_CONTROL_S3_BUCKET` - bucket containing block and object store access configuration
- `CATALOGUE_DATA_ACCESS_CONTROL_S3_BUCKET` - bucket containing catalogue access configuration
- `WORKFLOW_DATA_ACCESS_CONTROL_S3_BUCKET` - bucket containing workflow access configuration
- `EODH_CONFIG_DIR` - directory containing config information
- `PULSAR_TOPIC` - topic for pulsar messages
- `PULSAR_TOPIC_BULK` - topic for bulk pulsar messages
- `ENV_NAME` - name of environment
- `MAX_LOG_MESSAGES` - maximum number of log messages to display
- `MAX_ENTRIES` - maximum number of entries to send per message
- `RUNTIME_FREQUENCY_LIMIT` - minimum time (seconds) required between reharvests 
- `DEBUG` - debug mode enabled (bool)


### Query parameters

An optional `age` query can be added to the `/{workspace_name}/harvest_logs` POST endpoint to set a different age range e.g. POST `/{workspace_name}/harvest_logs?age=86400`. For more information, visit the OpenAPI docs.  


## Pulsar Messages


### Outgoing Pulsar Messages (`harvested` topic)

After processing, the service sends a message to the `harvested` topic. 

```json
{
  "id": "<unique_id>",
  "workspace": "<workspace_name>",
  "bucket_name": "<destination_s3_bucket>",
  "source": "<source_url_prefix>",
  "target": "<target_url_prefix>",
  "updated_keys": ["<list/of/updated/keys>"],
  "deleted_keys": ["<list/of/deleted/keys>"],
  "added_keys": ["<list/of/newly/added/keys>"]
}
```

- `bucket_name`: S3 bucket where files are stored (may be different from the source).
- `updated_keys`, `deleted_keys`, `added_keys`: Refer to the deleted, or added files in the output catalogue.

**Note:** The service may also send "empty" catalogue change messages (with no updated, deleted, or added keys) to indicate a successful harvest with no file changes.


## Usage

The service is typically run as part of a data pipeline, but you can invoke it directly for testing or development.

Run the file harvester from the command line:

```sh
fastapi dev app.py
```


## Development

- Code is in `workspace_file_harvester`.
- Formatting: [Black](https://black.readthedocs.io/), [Ruff](https://docs.astral.sh/ruff/), [isort](https://pycqa.github.io/isort/).
- Linting: [Pylint](https://pylint.pycqa.org/).
- Pre-commit checks are installed with `make setup`.

Useful Makefile targets:

- `make setup`: Set up or update the dev environment.
- `make test`: Run tests continuously.
- `make testonce`: Run tests once.
- `make lint`: Run all linters and formatters.
- `make requirements`: Update requirements files from `pyproject.toml`.
- `make requirements-update`: Update to the latest allowed versions.
- `make dockerbuild`: Build a Docker image.
- `make dockerpush`: Push a Docker image.


## Managing requirements

Requirements are specified in `pyproject.toml`, with development requirements listed separately. Specify version
constraints as necessary but not specific versions. After changing them:

* Run `pip-compile` (or `pip-compile -U` to upgrade requirements within constraints) to regenerate `requirements.txt`
* Run `pip-compile --extra dev -o requirements-dev.txt` (again, add `-U` to upgrade) to regenerate
  `requirements-dev.txt`.
* Run the `pip3 install -r requirements.txt` and `pip3 install -r requirements-dev.txt` commands again and test.
* Commit these files.

If you see the error

```commandline
Backend subprocess exited when trying to invoke get_requires_for_build_wheel
Failed to parse /.../template-python/pyproject.toml
```

then install and run `validate-pyproject pyproject.toml` and/or `pip3 install .` to check its syntax.

To check for vulnerable dependencies, run `pip-audit`.


## Testing

Run all tests with:

```sh
make testonce
```

Tests use [pytest](https://docs.pytest.org/), [moto](https://github.com/spulec/moto) for AWS mocking, and [requests-mock](https://requests-mock.readthedocs.io/).

## Troubleshooting

- **Authentication errors:** Ensure your `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` are set correctly and have permission to access the required S3 buckets.
- **Pulsar connection issues:** Check that `PULSAR_URL` is set to the correct broker address and is reachable from your environment.
- **S3 upload or download failures:** Verify that `S3_BUCKET` (and any other relevant buckets like `PATCH_BUCKET` or `S3_SPDX_BUCKET`) exist, your credentials have the correct permissions, and the bucket region matches your configuration.

Check the application logs for detailed error messages.


## Releasing

Ensure that `make lint` and `make test` work correctly and produce no further changes to code formatting before
continuing.

Releases tagged `latest` and targeted at development environments can be created from the `main` branch. Releases for
installation in non-development environments should be created from a Git tag named using semantic versioning. For
example, using

* `git tag 1.2.3`
* `git push --tags`

Normally, Docker images will be built automatically after pushing to the UKEODHP repos. Images can also be created
manually in the following way:

* For versioned images create a git tag.
* Log in to the Docker repository service. For the UKEODHP environment this can be achieved with the following command
  ```AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=... aws ecr get-login-password --region eu-north-1 | docker login --username AWS --password-stdin 312280911266.dkr.ecr.eu-west-2.amazonaws.com```
  You will need to create an access key for a user with permission to modify ECR first.
* Run `make dockerbuild` (for images tagged `latest`) or `make dockerbuild VERSION=1.2.3` for a release tagged `1.2.3`.
  The image will be available locally within Docker after this step.
* Run `make dockerpush` or `make dockerpush VERSION=1.2.3`. This will send the image to the ECR repository.


