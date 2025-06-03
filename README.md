# Workspace File Harvester

Collects user-uploaded files from S3. STAC files are passed to the transformer and ingestor for harvesting and access policy files are used to update public access of files, folders and workflows. 

# Development of this component

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


### Configuration

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


## Building and testing

This component uses `pytest` tests and the `ruff` and `black` linters. `black` will reformat your code in an
opinionated way.

A number of `make` targets are defined:
* `make test`: run tests continuously
* `make testonce`: run tests once
* `make lint`: lint and reformat
* `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild `VERSION=1.2.3` for a release image)
* `make dockerpush`: push a `latest` Docker image (again, you can add `VERSION=1.2.3`) - normally this should be done
  only via the build system and its GitHub actions.

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
