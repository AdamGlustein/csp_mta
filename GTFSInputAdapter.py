
from compiled_protobuf import gtfs_realtime_pb2

import logging
import pandas as pd
import requests
import time

from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def
from csp import ts
from datetime import timedelta
import threading
import time

'''
Reference for NYCT Subway GTFS feed: https://new.mta.info/document/134521
Reference for MTA LIRR/MetroNorth feed: https://raw.githubusercontent.com/OneBusAway/onebusaway-gtfs-realtime-api/master/src/main/proto/com/google/transit/realtime/gtfs-realtime-MTARR.proto
'''

LINE_TO_ENDPOINT = {
    '1234567S': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    'ACE': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    'BDFM': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    'G': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    'JZ': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    'NQRW': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    'L': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    'SI': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
    'LIRR': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr",
    'MNR': "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr"
}

MTA_FEED_UPDATE_TIME = timedelta(seconds=30)
GTFS_DIRECTION = ['', 'Uptown', '', 'Downtown', '']

# Stops: load into a dataframe
STOP_INFO_DF = pd.read_csv('stops.txt', index_col='stop_id')

logging.basicConfig(level=logging.INFO)

class GTFSRealtimeAdapterImpl(PushInputAdapter):
    def __init__(self, service):
        '''
        :param service: a string which specifies the services to subscribe to
        '''
        if service not in LINE_TO_ENDPOINT:
            raise ValueError(f'Given transit service {service} is unknown: supported services are all lines of NYC Subway, MTA LIRR, and MetroNorth (MNR)')

        self._logger = logging.getLogger()
        self._logger.info(f'Initializing GTFS-realtime adapter for service: {service}')

        self._service = service
        self._endpoint = LINE_TO_ENDPOINT[service]
        self._thread = None
        self._running = False

    def start(self, starttime, endtime):
        self._logger.info(f'Starting GTFS-realtime adapter using endpoint {self._endpoint}')
        self._running = True
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def stop(self):
        self._logger.info(f'Stopping GTFS-realtime adapter for endpoint {self._endpoint}')
        if self._running:
            self._running = False
            self._thread.join()

    def _run(self):
        while self._running:
            '''
            Tick out a list of GTFS messages for all services subscribed to
            '''
            response = requests.get(self._endpoint)
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            self.push_tick(feed)
            time.sleep(MTA_FEED_UPDATE_TIME.total_seconds())

GTFSRealtimeInputAdapter = py_push_adapter_def('GTFSRealtimeInputAdapter', GTFSRealtimeAdapterImpl, ts[object], service=str)
