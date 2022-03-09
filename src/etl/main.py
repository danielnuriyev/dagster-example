import logging
import os
import time

from contextlib import contextmanager

from dagster import job, op, repository, resource, sensor
from dagster import config_from_files
from dagster import Any, AssetMaterialization, DynamicOut, DynamicOutput, OpExecutionContext, RunRequest, ScheduleDefinition

from etl.sources import MySQLSource
from etl.sinks import S3Sink
from etl.caches import PickledCache

logger = logging.getLogger(__name__)

cache = PickledCache() # to remember where the pipeline stopped last time

@resource
@contextmanager
def source(context):
    c = MySQLSource(
        host=context.resource_config["host"],
        user=context.resource_config["user"],
        password=context.resource_config["password"],
        database=context.resource_config["database"],
        table=context.resource_config["table"],
        limit=100000,
        sleep=0,
        stream=True,  # for a small table that will not overfill the local storage, one can use False
        cache=cache,  # TODO: init cache from conf
    )
    try:
        yield c
    finally:
        c.close()


@op(required_resource_keys={"source"}, out=DynamicOut(Any))
def read_source(context):
    pid = os.getpid()
    for batch in context.resources.source.next_batch(): # any source must have next_batch method
        # If a pipeline is streaming (set in source above)
        # then next_batch will return only 1 batch
        # The next batch will be extracted when this pipeline runs again started by a sensor defined below
        timestamp = int(time.time() * 1000000)
        batch_id = id(batch)
        k = f"{timestamp}_{pid}_{batch_id}"
        yield DynamicOutput(value=batch, mapping_key=k) # non-unique k will be ignored


@resource
def sink(context):
    # any sink must implement sink(pandas.DataFrame) method
    return S3Sink(
        bucket=context.resource_config["bucket"],
        path=context.resource_config["path"],
    )


@op(required_resource_keys={"sink"})
def output_data(context: OpExecutionContext, batch: Any) -> Any:

    context.resources.sink.sink(batch)

    # this allows to create dependencies of other pipelines on this one
    # this can be emitted in a separate op
    yield AssetMaterialization(
        asset_key=f"any-key", # any key that uniquely identifies this pipeline
        description="Signal that this iteration is done",
        metadata={}, # this can identify this batch uniquely
    )


@job(resource_defs={"source": source, "sink": sink})
def main():
    batches = read_source()
    outputs = batches.map(output_data)
    outputs.collect()


# sensor is not needed for non-streaming pipelines
def init_sensor(job, conf, interval):
    @sensor(job=job, minimum_interval_seconds=interval)
    def next_batch():
        return RunRequest(
            run_key=str(time.time()),
            run_config=conf
        )
    return next_batch


# not used in this pipeline. just an example.
def init_schedule(job, conf):
    return ScheduleDefinition(
        job=job,
        cron_schedule="* * * * *",
        run_config=conf
    )


@repository()
def repo():
    # The idea is to load source, steps and sinks and build a pipeline dynamically.
    # The current code does not build pipelines dynamically :(
    local_conf_file = "test.yaml"
    interval = 30 # should be loaded from config
    conf = config_from_files([local_conf_file])
    sensor = init_sensor(main, conf, interval)
    # schedule = init_schedule(main, conf)
    return [main, sensor]
