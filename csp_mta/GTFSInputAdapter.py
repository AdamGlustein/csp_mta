import logging
import threading
from datetime import datetime
import time
import requests

from csp import ts
from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def

from .compiled_protobuf.gtfs_realtime_pb2 import FeedMessage
from .mta_util import *

__all__ = ("GTFSRealtimeInputAdapter",)

# Realtime push adapter


class GTFSRealtimeAdapterImpl(PushInputAdapter):
    def __init__(self, service, publish_raw_bytes):
        """Implementation for GTFS Realtime Adapter

        Args:
            service (str): services to subscribe to
            publish_raw_bytes (bool): used for recording data
        """
        if service not in LINE_TO_ENDPOINT:
            raise ValueError(
                f"Given transit service {service} is unknown: supported services are all lines of NYC Subway, MTA LIRR, and MetroNorth (MNR)"
            )

        self._service = service
        self._interval = MTA_FEED_UPDATE_TIME.total_seconds()
        self._raw = publish_raw_bytes
        self._endpoint = LINE_TO_ENDPOINT[service]
        self._thread = None
        self._running = False

    def start(self, starttime, endtime):
        self._endtime = endtime
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        if self._running:
            self._running = False
            self._thread.join()

    def _run(self):
        while self._running:
            """
            Tick out a list of GTFS messages for all services subscribed to
            """
            response = requests.get(self._endpoint)
            if self._raw:
                self.push_tick(response.content)  # raw bytes for recording
            else:
                feed = FeedMessage()
                feed.ParseFromString(response.content)
                self.push_tick(feed)
            sleep_interval = min(
                # edge case where we exit prematurely
                (self._endtime-datetime.utcnow()).total_seconds(), 
                self._interval
            )
            time.sleep(sleep_interval)


GTFSRealtimeInputAdapter = py_push_adapter_def(
    "GTFSRealtimeInputAdapter",
    GTFSRealtimeAdapterImpl,
    ts[FeedMessage],
    service=str,
    publish_raw_bytes=bool,
)
