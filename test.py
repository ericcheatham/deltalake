#!/usr/bin/python

import sentry_sdk
sentry_sdk.init(
    dsn="https://0a757290b45e471c96f703c1e4744ff7@o4504521748709376.ingest.sentry.io/4504521749954560",

    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0
)

division_by_zero = 1 / 0
