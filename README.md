# hammer

hammer is a command-line tool to schema management for Google Cloud Spanner.

![](https://github.com/daichirata/hammer/workflows/Test/badge.svg)

## Installation

Download the single-binary executable from:

https://github.com/daichirata/hammer/releases

or

``` shell
$ go install github.com/daichirata/hammer@latest
```

## Usage

```
$ hammer -h
hammer is a command-line tool to schema management for Google Cloud Spanner.

Usage:
  hammer [command]

Examples:

* Export spanner schema
  hammer export spanner://projects/projectId/instances/instanceId/databases/databaseName > schema.sql

* Apply local schema file
  hammer apply spanner://projects/projectId/instances/instanceId/databases/databaseName /path/to/file

* Apply schema from standard input
  cat /path/to/file | hammer apply spanner://projects/projectId/instances/instanceId/databases/databaseName -

* Create database and apply local schema (faster than running database creation and schema apply separately)
  hammer create spanner://projects/projectId/instances/instanceId/databases/databaseName /path/to/file

* Create database and apply schema from standard input
  cat /path/to/file | hammer create spanner://projects/projectId/instances/instanceId/databases/databaseName -

* Copy database
  hammer create spanner://projects/projectId/instances/instanceId/databases/databaseName1 spanner://projects/projectId/instances/instanceId/databases/databaseName2

* Compare local files
  hammer diff /path/to/file /another/path/to/file

* Compare schema from standard input against spanner schema
  cat /path/to/file | hammer diff - spanner://projects/projectId/instances/instanceId/databases/databaseName

* Compare local file against spanner schema
  hammer diff /path/to/file spanner://projects/projectId/instances/instanceId/databases/databaseName

* Compare spanner schema against spanner schema
  hammer diff spanner://projects/projectId/instances/instanceId/databases/databaseName1 spanner://projects/projectId/instances/instanceId/databases/databaseName2

Available Commands:
  apply       Apply schema
  create      Create database and apply schema
  diff        Diff schema
  export      Export schema
  help        Help about any command

Flags:
  -h, --help   help for hammer

Use "hammer [command] --help" for more information about a command.
```

The DSN must be given in the following format.

```
spanner://projects/{projectId}/instances/{instanceId}/databases/{databaseName}?credentials=/path/to/file.json
```

| Param          | Required |  Description                                                                                   |
| -------------- | -------- | ---------------------------------------------------------------------------------------------- |
| `projectId`    | true     | The Google Cloud Platform project id                                                           |
| `instanceId`   | true     | The id of the instance running Spanner                                                         |
| `databaseName` | true     | The name of the Spanner database                                                               |
| `credentials`  | false    | The path to the keyfile. If not present, client will use your default application credentials. |

### Using with the Spanner Emulator

hammer transparently honours the `SPANNER_EMULATOR_HOST` environment variable that the Cloud Spanner Go client SDK uses, so it can target the [Spanner emulator](https://cloud.google.com/spanner/docs/emulator) without any code or DSN changes.

1. Start the emulator (`gcloud emulators spanner start` or the official Docker image).
2. Create an instance and database against it once (see the [emulator documentation](https://cloud.google.com/spanner/docs/emulator)).
3. Export the env var and invoke hammer normally:

``` shell
$ export SPANNER_EMULATOR_HOST=localhost:9010
$ hammer apply spanner://projects/test-project/instances/test-instance/databases/test-database schema.sql
```

The `credentials=` query parameter is not needed when talking to the emulator.

### Flags

apply, create, diff and export can accept the flags defined below

```
--ignore-alter-database   ignore alter database statements
--ignore-change-streams   ignore change streams statements
--ignore-models           ignore model statements
--ignore-proto-bundles    ignore proto bundle statements
```

### Examples

Suppose you have an existing SQL schema like the following:

``` sql
CREATE TABLE users (
  user_id STRING(36) NOT NULL,
  email STRING(MAX),
) PRIMARY KEY(user_id);
```

And you want "upgrade" your schema to the following:

``` sql
CREATE TABLE users (
  user_id STRING(36) NOT NULL,
  email STRING(MAX) NOT NULL,
  age INT64,
  name STRING(MAX) NOT NULL,
) PRIMARY KEY(user_id);
CREATE INDEX idx_users_name ON users (name);
```

Hammer changes the schema by applying the following SQL to the spanner:

``` sql
hammer diff old.sql new.sql

UPDATE users SET email = '' WHERE email IS NULL;
ALTER TABLE users ALTER COLUMN email STRING(MAX) NOT NULL;
ALTER TABLE users ADD COLUMN age INT64;
ALTER TABLE users ADD COLUMN name STRING(MAX) NOT NULL DEFAULT ("");
ALTER TABLE users ALTER COLUMN name DROP DEFAULT;
CREATE INDEX idx_users_name ON users(name);
```

- When altering a nullable column to add the NOT NULL attribute, hammer update the column with the default value, and then add the NOT NULL attribute. (e.g. email)
- When adding a column with the NOT NULL attribute without the DEFAULT attribute, hammer add a default value to the column implicitly, and then drop the DEFAULT attribute. (e.g. name)

### Queues

Hammer changes options, row deletion policies, and the PARENT and ON DELETE clauses of a queue with ALTER QUEUE.
ALTER QUEUE cannot change columns, primary keys, or the parent table, so hammer drops and recreates the queue for these changes.
Hammer also drops and recreates a queue when it recreates the parent table of the queue.
The recreation deletes all messages in the queue.
Spanner does not fully support dropping and recreating a queue with the same name, and the `RECEIVE` function of the queue may take some time to become available again.
See [Spanner queues overview](https://cloud.google.com/spanner/docs/queues/queues-overview) for details.

## LICENSE

Unless otherwise noted, the hammer source files are distributed under the MIT License found in the LICENSE file.
