import logging
import threading
import time

import httpx
from csp import ts
from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def

from .compiled_protobuf import gtfs_realtime_pb2
from .mta_util import *

logging.basicConfig(level=logging.INFO)

__all__ = ("GTFSRealtimeInputAdapter",)


class GTFSRealtimeAdapterImpl(PushInputAdapter):
    def __init__(self, service):
        """Implementation for GTFS Realtime Adapter

        Args:
            service (str): services to subscribe to
        """
        if service not in LINE_TO_ENDPOINT:
            raise ValueError(
                f"Given transit service {service} is unknown: supported services are all lines of NYC Subway, MTA LIRR, and MetroNorth (MNR)"
            )

        self._logger = logging.getLogger()
        self._logger.info(f"Initializing GTFS-realtime adapter for service: {service}")

        self._service = service
        self._endpoint = LINE_TO_ENDPOINT[service]
        self._thread = None
        self._running = False

    def start(self, starttime, endtime):
        self._logger.info(
            f"Starting GTFS-realtime adapter using endpoint {self._endpoint}"
        )
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._logger.info(
            f"Stopping GTFS-realtime adapter for endpoint {self._endpoint}"
        )
        if self._running:
            self._running = False
            self._thread.join()

    def _run(self):
        while self._running:
            """
            Tick out a list of GTFS messages for all services subscribed to
            """
            response = httpx.get(self._endpoint)
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            self.push_tick(feed)
            time.sleep(MTA_FEED_UPDATE_TIME.total_seconds())


GTFSRealtimeInputAdapter = py_push_adapter_def(
    "GTFSRealtimeInputAdapter", GTFSRealtimeAdapterImpl, ts[object], service=str
)
