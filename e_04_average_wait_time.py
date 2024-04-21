# This example uses historical data to calculate the average wait time at a stop at different hours throughout the day

import argparse
import csp
from csp.adapters.parquet import ParquetReader
from csp_mta.compiled_protobuf import gtfs_realtime_pb2

from matplotlib import pyplot as plt
from datetime import datetime, timedelta


@csp.node
def raw_bytes_to_gtfs_message(raw: csp.ts[str]) -> csp.ts[object]:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw.encode("latin-1"))
    return feed


@csp.graph
def hourly_average_wait_time(filename: str, stop_id: str) -> csp.ts[object]:
    raw_bytes = ParquetReader(
        filename_or_list=filename, time_column="time"
    ).subscribe_all(typ=str, field_map="msg")
    gtfs = raw_bytes_to_gtfs_message(raw_bytes)
    return gtfs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--filename",
        type=str,
        default=None,
        required=True,
        help="File which stores the recorded data",
    )
    parser.add_argument(
        "--stop_id",
        type=str,
        default=None,
        required=True,
        help="stop_id from data/stops.txt",
    )
    args = parser.parse_args()

    res = csp.run(
        hourly_average_wait_time,
        args.filename,
        args.stop_id,
        starttime=datetime(2024, 4, 21, 12),
        endtime=datetime(2024, 4, 21, 18),
    )
    print(res)
