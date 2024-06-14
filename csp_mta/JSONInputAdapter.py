import threading
import time
from datetime import datetime, timedelta

import requests
from csp import ts
from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def

__all__ = ("JSONRealtimeInputAdapter",)


class JSONRealtimeAdapterImpl(PushInputAdapter):
    def __init__(self, endpoint, interval, publish_raw_bytes):
        self._endpoint = endpoint
        self._interval = interval.total_seconds()
        self._raw = publish_raw_bytes
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
            response = requests.get(self._endpoint)
            if self._raw:
                self.push_tick(response.content)  # raw bytes for recording
            else:
                self.push_tick(response.json())
            sleep_interval = min(
                # edge case where we exit prematurely
                (self._endtime-datetime.utcnow()).total_seconds(), 
                self._interval
            )
            time.sleep(sleep_interval)


JSONRealtimeInputAdapter = py_push_adapter_def(
    "JSONRealtimeAdapter",
    JSONRealtimeAdapterImpl,
    ts[object],
    endpoint=str,
    interval=timedelta,
    publish_raw_bytes=bool,
)
