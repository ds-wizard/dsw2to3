# dsw2to3

CLI tool to support data migration from DSW 2.14 to DSW 3.0

## Usage

### Prerequisites

* DSW 3.0
* MongoDB (with DSW 2.14 data)
* PostgreSQL (with initial DSW 3.0 structure)
* S3 storage (e.g. [Minio](https://min.io))
* Python 3.6+ (recommended to use [virtual environment](https://docs.python.org/3/library/venv.html))

The machine where you are going to execute the migration tool must have access to MongoDB, PostgreSQL, and S3 storage. See `examples/docker-compose.yml` for reference.

You need to run DSW 3.0 at least once before the data migration, so it initializes your PostgreSQL database (it will create tables and initial data). You can try to log-in with the default user to check if it is initialized correctly.

Don't hesitate to consult with us if unclear.

### Installation

You can install the tool using PyPI:

```shell
$ python -m venv env
$ . env/bin/activate
(env) $ pip install dsw2to3
...
(env) $ dsw2to3 --help
```

Or using this repository:

```shell
$ git clone https://github.com/ds-wizard/dsw2to3.git
$ python -m venv env
$ . env/bin/activate
(env) $ pip install .
...
(env) $ dsw2to3 --help
```

### Important notes

- Migration tool must have access to MongoDB database (data source), PostgreSQL database and S3 storage (target). It needs to be configured in `config.yml`. During the migration (e.g. from DSW or other tool), the data must not change to avoid inconsistency.
- Migration tool does not make any changes in MongoDB, it only reads data from there.
- Migration tool checks if target PostgreSQL database is in expected state (after fresh installation of DSW 3.0).
- Migration tool initially deletes all data from PostgreSQL database before migrating to avoid duplication and inconsistency (for regular use it just removes the default data, e.g., default users).
- Migration tool initially deletes all objects in configured S3 bucket. If the bucket does not exist, it tries to create a new one.
- Migration tool migrates data from MongoDB to PostgreSQL in expected way for DSW as well as from MongoDB (GridFS) to S3 storage.
- You can run the tool with `--dry-run` to check what it will do. During dry run, nothing is deleted, changed, or added (no SQL transactions are committed).

### Steps

1. Prepare `config.yml` for the migration based on your setup (see `config.example.yml`)
2. Stop DSW in order to prevent changes in data during the migration
3. Run `dsw2to3 -c path/to/config.yml --dry-run` to see how it will work with your configuration
3. Run `dsw2to3 -c path/to/config.yml` (see `dsw2to3 --help` for more options)
4. After migration, run DSW 3.0 and check the migrated data
5. Archive data from MongoDB (e.g. using [mongodump](https://docs.mongodb.com/manual/reference/program/mongodump/))
6. Clean up your deployment (get rid of unused services and configuration files)

In case of error during the migration, follow the details from logs. You can run it with `--best-effort` flag that will skip errors (just log them out).

## Questions and Discussion

If anything is unclear, or you need help, let us know via issue in this repository.

## License

This project is licensed under the Apache License v2.0 - see the
[LICENSE](LICENSE) file for more details.
