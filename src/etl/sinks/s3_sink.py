
import io
import logging
import os
import psutil
import time

import boto3

from etl.sinks.sink import Sink

logger = logging.getLogger(__name__)

class S3Sink(Sink):

    def __init__(
            self,
            bucket,
            path,
            object_name_func=None,
            schema=None,
            session=None,
    ):

        print("SINK")

        self.bucket = bucket

        if len(path) > 0 and path[0] == "/":
            path = path[1:]

        self.path = path

        self.object_name_func = object_name_func
        self.schema = schema
        self._s3_client = session.client('s3') if session else boto3.client('s3')
        self._pid = os.getpid()
        self._rows = 0
        self._start = time.time()

    def sink(self, packet):

        logger.info(type(packet))

        if packet is None:
            return
        # if packet is None or not packet.get("data", None) or not isinstance(packet["data"], pandas.DataFrame):
        #    logger.warn(f"Wrong data type: {type(packet)}")
        #    return

        df = packet["data"]

        if len(df) < 1:
            return

        timestamp = int(time.time() * 1000000)

        if self.object_name_func:
            object_name = self.object_name_func(self, df)
        else:
            object_name = self.path
            if not self.path.endswith("/"):
                object_name += "/"
            object_name += f"{timestamp}-{self._pid}-{id(df)}.parquet"

        logger.info(object_name)

        buffer = io.BytesIO()
        if self.schema:
            df.to_parquet(buffer, schema=self.schema)
        else:
            df.to_parquet(buffer)
        buffer.seek(0)
        logger.info(f"File size: {buffer.getbuffer().nbytes}")
        response = self._s3_client.put_object( # TODO: this can run in a thread
            Body=buffer,
            Bucket=self.bucket,
            Key=object_name
        )
        self._rows += len(df)
        logger.info(f"Piped {self._rows} in {int(time.time() - self._start)} seconds")

        pid = os.getpid()
        process = psutil.Process(pid)
        memory = process.memory_info().rss
        logger.info(f'Memory: {"{:,}".format(memory)}')

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error(f"Failed to upload to bucket {self.bucket} object {object_name}: {response}")
            raise response

