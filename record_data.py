# Utility to record data from the realtime feeds as they don't have publicly available recordings on the MTA website
import argparse
import csp
from csp.adapters.parquet import ParquetWriter
import httpx
import random
import time
from datetime import datetime, timedelta

from csp_mta import (
    MTA_FEED_UPDATE_TIME,
    GTFSRealtimeInputAdapter,
    JSONRealtimeInputAdapter,
)


@csp.node
def cast_to_str(data: csp.ts[object]) -> csp.ts[str]:
    # Data will always be bytes, which is non-native
    return data.decode("latin-1")


@csp.graph
def record(filename: str, service: str = "", endpoint: str = ""):
    # Simple graph to record data and write to Parquet
    if service and endpoint or not (service or endpoint):
        raise ValueError(
            "Exactly one of service (GTFS) and endpoint (JSON) can be used"
        )

    if service:
        raw_bytes = GTFSRealtimeInputAdapter(service, True)
    else:
        raw_bytes = JSONRealtimeInputAdapter(
            endpoint, MTA_FEED_UPDATE_TIME.total_seconds(), True
        )

    msg = cast_to_str(raw_bytes)
    pq = ParquetWriter(file_name=filename, timestamp_column_name="time")
    pq.publish("msg", msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--service",
        type=str,
        default=None,
        help="Service to record data from for GTFS feeds",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default=None,
        help="Endpoint to record data from for JSON feeds",
    )
    parser.add_argument(
        "--out_file", type=str, default=None, help="Output file to store raw bytes"
    )
    parser.add_argument(
        "--minutes_to_run", type=int, default=None, help="Minutes to run for"
    )

    args = parser.parse_args()
    duration = timedelta(minutes=args.minutes_to_run)

    print(f"\nWriting data from {datetime.now()} to {datetime.now()+duration}\n")
    csp.run(
        record,
        args.out_file,
        args.service or "",
        args.endpoint or "",
        starttime=datetime.utcnow(),
        endtime=duration,
        realtime=True,
    )
    print(f"\nDone writing data...\n")
