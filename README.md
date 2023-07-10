## What it is

This is an example that pipes data from MySQL to S3.
MySQL and S3 are just examples. 
They can be replaced by any source and sink as long as they match the interface.

There are 2 ways to pipe a table: fully or in batches.
Piping in batches is required for large tables.

Piping in batches has the following problem: 
the first operation that outputs batches must finish before the nest ops can start.
If the table is large, all these batches will be piped out of the original table into the local storage.
This is not scaleable. 

The solution is to extract 1 batch and then rerun the pipeline to extract the next batch etc.

To do so the pipeline must:
- remember where it stopped last time
- use Dagster Sensor

## Code

First you must set up the environment by running:

```shell
. ./setup.sh
```

The entry point is in `src/main.py`. Learn the code. 
To run it, you'll need to create a `conf.yaml` file with configurations. 

Then run the Dagster Daemon:

```shell
dagster-daemon run &
```

If you omit `&` the daemon will stay in teh foreground. This is useful for learnig Dagster.

Then run the UI:

```shell
dagit -w workspace.yaml
```

Browse to the UI and enable the sensor.
