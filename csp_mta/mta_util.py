"""
Reference for NYCT Subway GTFS feed: https://new.mta.info/document/134521
Reference for MTA LIRR/MetroNorth feed: https://raw.githubusercontent.com/OneBusAway/onebusaway-gtfs-realtime-api/master/src/main/proto/com/google/transit/realtime/gtfs-realtime-MTARR.proto
Useful data for transfers, trains, etc.: https://transitfeeds.com/p/mta/79/latest/file/transfers.txt
"""

import os
import os.path
from datetime import timedelta

import pandas as pd
import pytz

_DATA_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")

__all__ = (
    "LINE_TO_ENDPOINT",
    "MTA_FEED_UPDATE_TIME",
    "GTFS_DIRECTION",
    "NYC_TIMEZONE",
    "TOTAL_SUBWAY_STATIONS",
    "ADA_ACCESSIBLE_STATIONS",
    "STOP_INFO_DF",
    "TRANSFER_INFO_DF",
    "ACCESSIBILITY_ENDPOINT",
    "ALERT_ENDPOINTS",
)


LINE_TO_ENDPOINT = {
    "1234567S": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "ACE": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "BDFM": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "JZ": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    "NQRW": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "L": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "SI": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
    "LIRR": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr",
    "MNR": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/mnr%2Fgtfs-mnr",
}

MTA_FEED_UPDATE_TIME = timedelta(seconds=30)
GTFS_DIRECTION = ["", "Uptown", "", "Downtown", ""]
NYC_TIMEZONE = pytz.timezone("America/New_York")

# Stops and transfers: load into a dataframe
TOTAL_SUBWAY_STATIONS = 472
ADA_ACCESSIBLE_STATIONS = 113

STOP_INFO_DF = pd.read_csv(os.path.join(_DATA_DIR, "stops.csv"), index_col="stop_id")
TRANSFER_INFO_DF = pd.read_csv(os.path.join(_DATA_DIR, "transfers.csv"))

# Realtime elevator/escalator status
ACCESSIBILITY_ENDPOINT = (
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fnyct_ene.json"
)

# Alert endpoints
ALERT_ENDPOINTS = {
    "bus": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fbus-alerts.json",
    "mnr": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fmnr-alerts.json",
    "lirr": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Flirr-alerts.json",
    "subway": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json",
    "all": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fall-alerts.json",
}
