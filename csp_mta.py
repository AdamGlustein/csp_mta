import csp
from csp import ts

from csp.impl.adaptermanager import AdapterManagerImpl
from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def

from datetime import datetime, timedelta
from nyct_gtfs import NYCTFeed
import threading
from typing import List

class TrainData(csp.Struct):
    line_str : str      # represents the service being run i.e. 1, 2, N, 7 etc.
    northbound : bool   # True if NB, False if SB (see MTA documentation for how direction is defined on E-W lines)
    current_stop: int   # id of the current stop
    next_stop: int      # id of the next stop

# Map each line API endpoint to an ID so we don't duplicate feeds
LINE_DATA_GROUPINGS = {
    line : 1 for line in ["1", "2", "3", "4", "5", "6", "7", "S", "GS"]
} + {
    line : 2 for line in ["A", "C", "E", "H", "FS", "SF", "SR"]
} + {
    line : 3 for line in ["B", "D", "F", "M"]
} + {
    line : 4 for line in ["N", "Q", "R", "W"]
} + {
    line : 5 for line in ["J", "Z"]
} + {
    "G" : 6, "L": 7, 
}


def get_api_key_from_file(filename: str) -> str:
    with open(filename, 'r') as api_key_file:
        key = api_key_file.read()
    return key

MTA_API_KEY = get_api_key_from_file("api_key.txt")

class LineFeedWrapper:
    def __init__(self, feed: NYCTFeed=None, last_update: datetime=None):
        self._feed = feed
        self._last_update = last_update

class MTARealtimeAdapterManager:
    def __init__(self):
        pass

    def subscribe(self, line: str ):
        return MTAPushAdapter(self, line)
    
    def _create(self, engine, memo):
        return MTARealtimeAdapterManagerImpl(engine)

class MTARealtimeAdapterManagerImpl(AdapterManagerImpl):
    def __init__(self, engine) -> None:
        super().__init__(engine)
        self._inputs = {}
        self._feeds = {}
        self._running = False
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self):
        if self._running:
            self._running = False
            self._thread.join()

    def register_input_adapter(self, line, api_key, adapter):
        if line not in self._inputs:
            self._inputs[line] = []
            self._feeds[line] = LineFeedWrapper()
        self._inputs[line].append(adapter)
        # Subscribe to new feed
        group = LINE_DATA_GROUPINGS[line]
        if group not in self._feeds:
            self._feeds[group]._feed = NYCTFeed(line, api_key)
            self._feeds[group]._feed.refresh()
            self._last_update = self._feeds[group]._last_update

    def _run(self):
        lines = list(self._lines.keys())
        while self._running:
            for line in lines:
                group = LINE_DATA_GROUPINGS[line]
                feed = self._feeds[group]._feed
                feed.refresh()
                if feed.last_generated > self._feeds[group]._last_update:
                    # New update - push to inputs
                    adapters = self._inputs[line]
                    data = TrainData() # empty for now TO-DO
                    for adapter in adapters:
                        adapter.push_tick(data)


class MTAPushAdapterImpl(PushInputAdapter):
    def __init__(self, manager_impl, line, api_key):
        manager_impl.register_input_adapter(line, api_key, self)
        super().__init__()

MTAPushAdapter = py_push_adapter_def( "MTARealtimeData", MTAPushAdapterImpl, ts[TrainData], MTARealtimeAdapterManager, line=str, api_key=str )