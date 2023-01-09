import os
import logging
import sys

from turbine.runtime import RecordList
from turbine.runtime import Runtime

import pandas as pd

from deltalake import DeltaTable
from deltalake.writer import write_deltalake


logging.basicConfig(level=logging.INFO)




def write_records(data: dict):

    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"), 
        "AWS_SECRET_ACCESS_KEY":os.getenv("AWS_SECRET_ACCESS_KEY"), 
        "AWS_REGION": os.getenv("AWS_REGION"),
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    

    try:
        dt = DeltaTable(
        "s3://cheatham-s3-testing/deltas2/", 
        storage_options=storage_options
    )
        write_deltalake(
            table_or_uri=dt, 
            data=pd.DataFrame(data=data), 
            mode='update'
        )
    except:
         write_deltalake(
            table_or_uri="s3://cheatham-s3-testing/deltas2/", 
            data=pd.DataFrame(data=data),
            storage_options=storage_options
        )

def write_to_delta(records: RecordList) -> RecordList:

    data = {}

    for record in records:
        logging.info(f"input: {record}")

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
            source = await turbine.resources("pg")

            records = await source.records("customer_order")

            turbine.register_secrets("AWS_ACCESS_KEY_ID")
            turbine.register_secrets("AWS_SECRET_ACCESS_KEY")
            turbine.register_secrets("AWS_REGION")

            _ = await turbine.process(records, write_to_delta)

            destination_db = await turbine.resources("pg")

            await destination_db.write(records, "collection_archive", {})
        except Exception as e:
            print(e, file=sys.stderr)
