import logging
import os
import sys

import pandas as pd
from turbine.runtime import RecordList, Runtime

from deltalake import DeltaTable
from deltalake import writer
logging.basicConfig(level=logging.INFO)

S3_URI="s3://cheatham-s3-testing/deltas2/"

def write_records(data: dict):

    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION"),
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

   
    # Try to write to a DeltaTable. Catch the error if it does not exist and attempt 
    # to create the table 
    # try:
    dt = DeltaTable(
        S3_URI, storage_options=storage_options
    )
    writer.write_deltalake(
        table_or_uri=dt, 
        data=pd.DataFrame(data=data), 
        mode="update"
    )
    # except:
    #     writer.write_deltalake(
    #         table_or_uri=S3_URI,
    #         data=pd.DataFrame(data=data),
    #         storage_options=storage_options,
    #     )

def write_to_delta(records: RecordList) -> RecordList:
    """
    In order to write to a DeltaTable using delta-rs we need to convert our Record/Fixture
    data into a [DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html)

    We are stepping through all available records and parsing out the value in `payload`. The 
    key in `payload` corresponds to the column name, the value is the row value. 
    """
    data = {}

    for record in records:

        payload = record.value["payload"]
        for key, val in payload.items():
            if key in data:
                data[key].append(val)
            else:
                data.update({key: [val]})

    write_records(data=data)

    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            source = await turbine.resources("source_name")

            records = await source.records("customer_order")

            turbine.register_secrets("AWS_ACCESS_KEY_ID")
            turbine.register_secrets("AWS_SECRET_ACCESS_KEY")
            turbine.register_secrets("AWS_REGION")

            _ = await turbine.process(records, write_to_delta)

            destination_db = await turbine.resources("pg")

            await destination_db.write(records, "collection_archive", {})
        except Exception as e:
            print(e, file=sys.stderr)
