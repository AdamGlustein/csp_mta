
# This example includes a basic adapter for the MTA realtime elevator/escalator status feed, which is a plain JSON feed
# You are invited to explore other MTA accessbility feeds, which are listed here: https://api.mta.info/#/EAndEFeeds

from datetime import datetime, timedelta
import threading
import time
import requests

from csp.impl.pushadapter import PushInputAdapter
from csp.impl.wiring import py_push_adapter_def
from mta_util import *

import json
import csp

class OutageStats(csp.Struct):
    num_elevators_out : int=0
    num_stations_no_longer_ADA_accessible : int=0
    average_downtime_per_outage : timedelta=timedelta(seconds=-1)

class RTElevatorStatusAdapterImpl(PushInputAdapter):
    def __init__(self):
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
            response = requests.get(ACCESSIBILITY_ENDPOINT)
            json_str = response.content.decode('utf-8')
            json_dict = json.loads(json_str)
            self.push_tick(json_dict)
            time.sleep(MTA_FEED_UPDATE_TIME.total_seconds())

RTElevatorStatusAdapter = py_push_adapter_def('RTElevatorStatusAdapter', RTElevatorStatusAdapterImpl, csp.ts[object])

@csp.node
def elevator_outages(json_feed: csp.ts[object]) -> csp.ts[OutageStats]:
    stats = OutageStats()
    total_outage_time = timedelta()
    for outage in json_feed:
        if outage['equipmenttype'] == 'EL' and outage['isupcomingoutage'] == 'N': 
            # elevators, not escalators; only current, not planned outages
            stats.num_elevators_out += 1
            if outage['ADA'] == 'N':
                stats.num_stations_no_longer_ADA_accessible += 1
            
            # record time of outage
            date_format = '%m/%d/%Y %I:%M:%S %p'
            start_of_outage = datetime.strptime(outage['outagedate'], date_format)
            end_of_outage = datetime.strptime(outage['estimatedreturntoservice'], date_format)
            total_outage_time += ( end_of_outage - start_of_outage ) 

    stats.average_downtime_per_outage = total_outage_time / stats.num_elevators_out
    return stats

@csp.node
def repr_accessibility_stats(stats: csp.ts[OutageStats]) -> csp.ts[str]:
    s = f'\nTotal elevator outages: {stats.num_elevators_out}\n'
    s += f'ADA critical elevator outages: {stats.num_stations_no_longer_ADA_accessible}\n'
    s += f'Realtime Accessible Stations: {ADA_ACCESSIBLE_STATIONS-stats.num_stations_no_longer_ADA_accessible} of {TOTAL_SUBWAY_STATIONS}\n'
    s += f'Average Time per Outage: {stats.average_downtime_per_outage.days} days\n'
    return s

@csp.graph
def realtime_accessibility_stats():
    realtime_elevator_status = RTElevatorStatusAdapter()
    current_elevator_outages = elevator_outages(realtime_elevator_status)
    status = repr_accessibility_stats(current_elevator_outages)
    csp.print('Current Accessibility Status', status)

if __name__ == '__main__':
    csp.run(realtime_accessibility_stats, starttime=datetime.utcnow(), endtime=timedelta(seconds=10), realtime=True)
