import os

import pandas as pd

from deltalake import writer

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "AWS_REGION": os.getenv("AWS_REGION"),
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


S3_URI = os.getenv("AWS_URI")


def write_records(data: dict):

    """
    An example of attempting to write to a delta table. This will attempt to
    initialize the table if it does not already exist
    """
    try:
        maybe_table = writer.try_get_deltatable(S3_URI, storage_options=STORAGE_OPTIONS)

        if not maybe_table:
            writer.write_deltalake(
                table_or_uri=S3_URI,
                storage_options=STORAGE_OPTIONS,
                data=pd.DataFrame(data),
            )
        else:
            writer.write_deltalake(
                table_or_uri=maybe_table, data=pd.DataFrame(data), mode="append"
            )
    except:
        return
