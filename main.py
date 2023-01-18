import os
import logging
import sys

import sentry_sdk
from turbine.runtime import RecordList, Runtime

import utils

logging.basicConfig(level=logging.INFO)

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    traces_sample_rate=0.5,
)


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

    """
    Turbine functions are able to access code in other modules within your application
    """
    utils.write_records(data=data)

    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:

            """
            Register S3 secrets with your application so they are
            available at run time
            """
            turbine.register_secrets("AWS_ACCESS_KEY_ID")
            turbine.register_secrets("AWS_SECRET_ACCESS_KEY")
            turbine.register_secrets("AWS_REGION")
            turbine.register_secrets("AWS_URI")
            turbine.register_secrets("SENTRY_DSN")

            """
            Connect your turbine application to your data 
            source of choice (in this case, a postgres database)
            """
            source = await turbine.resources("pg")

            """
            Stream rows from source resource in the form of 
            records
            """
            records = await source.records("user_activity")

            """
            Use turbine function (defined above) to write to a delta
            table. 
            """
            processed = await turbine.process(records, write_to_delta)

            destination_db = await turbine.resources("flake")

            await destination_db.write(processed, "deltaprocessed", {})
        except Exception as e:
            print(e, file=sys.stderr)
