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

### Prerequisites

- Python 3.13
- [uv](https://docs.astral.sh/uv/)

### Install via makefile

```commandline
make setup
```

This will install dependencies using `uv sync` and set up `pre-commit` hooks.

It's safe and fast to run `make setup` repeatedly as it will only update things if
they have changed.

After `make setup` you can run `make pre-commit` to run pre-commit checks on staged changes and
`make pre-commit-all` to run them on all files. This replicates the linter checks that
run from GitHub actions.


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
fastapi dev workspace_file_harvester/app.py
```


## Development

- Code is in `workspace_file_harvester`.
- Formatting and linting: [Ruff](https://docs.astral.sh/ruff/).
- Type checking: [Pyright](https://microsoft.github.io/pyright/).
- Pre-commit checks are installed with `make setup`.

Useful Makefile targets:

- `make setup`: Set up or update the dev environment.
- `make test`: Run tests continuously with pytest-watcher.
- `make testonce`: Run tests once.
- `make check`: Run all linters, formatters, and type checks.
- `make format`: Auto-fix lint issues and format code.
- `make install`: Install dependencies from lockfile (frozen).
- `make update`: Update dependencies.
- `make dockerbuild`: Build a Docker image.
- `make dockerpush`: Push a Docker image.


## Managing dependencies

Dependencies are specified in `pyproject.toml`. After changing them:

* Run `uv sync` to update `uv.lock`.
* Test that everything works.
* Commit both `pyproject.toml` and `uv.lock`.

To validate the pyproject.toml syntax, run `make check` (includes `validate-pyproject`).


## Testing

Run all tests with:

```sh
make testonce
```

Tests use [pytest](https://docs.pytest.org/), [moto](https://github.com/spulec/moto) for AWS mocking, and [requests-mock](https://requests-mock.readthedocs.io/).

## Troubleshooting

- **Authentication errors:** Ensure your `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY` are set correctly and have permission to access the required S3 buckets.
- **Pulsar connection issues:** Check that `PULSAR_URL` is set to the correct broker address and is reachable from your environment.
- **S3 upload or download failures:** Verify that all relevant buckets exist, your credentials have the correct permissions, and the bucket region matches your configuration.

Check the application logs for detailed error messages.


## Releasing

Ensure that `make check` and `make testonce` work correctly and produce no further changes to code formatting before
continuing.

Releases tagged `latest` and targeted at development environments can be created from the `main` branch. Releases for
installation in non-development environments should be created from a Git tag named using semantic versioning. For
example, using

* `git tag v1.2.3`
* `git push --tags`

Docker images will be built automatically by GitHub Actions after pushing to the EO-DataHub repos.

Images can also be created manually:

* Run `make dockerbuild` (for images tagged `latest`) or `make dockerbuild VERSION=1.2.3` for a release tagged `1.2.3`.
  The image will be available locally within Docker after this step.
* Run `make dockerpush` or `make dockerpush VERSION=1.2.3`. This will send the image to the ECR repository.
