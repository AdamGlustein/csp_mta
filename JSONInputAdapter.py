import threading
from datetime import timedelta
import json
import time
import requests

from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def
from csp import ts

class JSONAdapterImpl(PushInputAdapter):
    def __init__(self, endpoint, interval):
        self._endpoint = endpoint
        self._interval = interval
        self._thread = None
        self._running = False

    def start(self, starttime, endtime):
        self._running = True
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self):
        if self._running:
            self._running = False
            self._thread.join()

    def _run(self):
        while self._running:
            response = requests.get(self._endpoint)
            json_str = response.content.decode('utf-8')
            json_dict = json.loads(json_str)
            self.push_tick(json_dict)
            time.sleep(self._interval.total_seconds())

JSONInputAdapter = py_push_adapter_def('JSONAdapter', JSONAdapterImpl, ts[object], endpoint=str, interval=timedelta)