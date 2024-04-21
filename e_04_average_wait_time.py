# This example uses historical data to calculate the average wait time at a stop at different hours throughout the day

import argparse
import csp
from csp.adapters.parquet import ParquetReader
from csp_mta import gtfs_realtime_pb2, STOP_INFO_DF

from datetime import datetime, timedelta
import pandas as pd


@csp.node
def raw_bytes_to_gtfs_message(raw: csp.ts[str]) -> csp.ts[object]:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw.encode("latin-1"))
    return feed


def get_stop_time_at_station(entity, stop_id, direction):
    """
    Helper Python function to get the stop time at a station
    Same as e_01 helper except that this one is directional
    """
    if entity.HasField("trip_update"):
        stop_time_updates = entity.trip_update.stop_time_update
        for update in stop_time_updates:
            # could be N or S
            if (
                update.stop_id == stop_id + direction
                and datetime.fromtimestamp(update.arrival.time) >= datetime.now()
            ):
                return update.arrival.time
    return None


@csp.node
def wait_time(feed_msgs: csp.ts[object], stop_id: str) -> csp.Outputs(
    uptown_wait=csp.ts[timedelta], downtown_wait=csp.ts[timedelta]
):
    """
    Posted wait time in both directions from the stop - not really "true" wait time since we don't check
    if the trains actually arrived when they said they would.
    """
    n_min, s_min = timedelta.max, timedelta.max
    for entity in feed_msgs.entity:
        for direction in ("N", "S"):
            t = get_stop_time_at_station(entity, stop_id, direction)
            if t is not None:
                wait = datetime.fromtimestamp(t) - datetime.now()
                if wait >= timedelta(0):
                    if direction == "N":
                        n_min = min(n_min, wait)
                    else:
                        s_min = min(s_min, wait)

    if n_min != timedelta.max and s_min != timedelta.max:
        return csp.output(uptown_wait=n_min, downtown_wait=s_min)


@csp.graph
def wait_time_distribution(filename: str, stop_id: str) -> csp.Outputs(
    mean=csp.ts[float], std=csp.ts[float]
):
    raw_bytes = ParquetReader(
        filename_or_list=filename, time_column="time"
    ).subscribe_all(typ=str, field_map="msg")
    gtfs = raw_bytes_to_gtfs_message(raw_bytes)
    wait_times = wait_time(gtfs, stop_id)

    # Calculate the average and standard deviation for each bucket
    bidirectional_wait_times = csp.flatten(
        [
            csp.apply(wait_times.uptown_wait, lambda x: x.total_seconds(), float),
            csp.apply(wait_times.downtown_wait, lambda x: x.total_seconds(), float),
        ]
    )
    avg_wait_time = csp.stats.mean(
        bidirectional_wait_times,
        interval=None,
        min_window=None,
    )
    std_wait_time = csp.stats.stddev(
        bidirectional_wait_times,
        interval=None,
        min_window=None,
    )

    return csp.output(mean=avg_wait_time, std=std_wait_time)


def format_time(td):
    minutes = round(td // 60)
    seconds = round(int(td) - minutes * 60)
    if minutes > 0:
        return f"{minutes} min {seconds} s"
    return f"{seconds} s"


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

    start = datetime(2024, 4, 21, 17)
    end = datetime(2024, 4, 21, 18)
    res = csp.run(
        wait_time_distribution,
        args.filename,
        args.stop_id,
        starttime=start,
        endtime=end,
    )

    # Cumulative stats at station at the end of the window
    mean_wait = seconds = res["mean"][-1][1]
    std_wait = seconds = res["std"][-1][1]
    format_str = "%Y-%m-%d %H:%M:%S"
    print(
        f'\nStation {STOP_INFO_DF.loc[args.stop_id, "stop_name"]}'
        + f"\nBetween {start.strftime(format_str)} and {end.strftime(format_str)}"
        + f"\nAverage wait time {format_time(mean_wait)} +/- {format_time(std_wait)}"
    )
