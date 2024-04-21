# This example prints all posted MTA bus or rail alerts

from datetime import datetime, timedelta

import csp

from csp_mta import ALERT_ENDPOINTS, MTA_FEED_UPDATE_TIME, JSONRealtimeInputAdapter


@csp.node
def pretty_print_alerts(alerts: csp.ts[object]) -> csp.ts[str]:
    all_alerts = []
    for entity in alerts["entity"]:
        route_id = entity["alert"]["informed_entity"][0]["route_id"]
        all_alerts.append(
            f'-- {route_id}: {entity["alert"]["header_text"]["translation"][0]["text"]}'
        )

    all_alerts.sort()
    delimiter = "\n\n"
    return "\n" + delimiter.join(all_alerts)


@csp.graph
def get_alerts(endpoint: str):
    alert_adapter = JSONRealtimeInputAdapter(endpoint, MTA_FEED_UPDATE_TIME)
    alert_panel = pretty_print_alerts(alert_adapter)
    csp.print("Realtime Alerts", alert_panel)


if __name__ == "__main__":
    csp.run(
        get_alerts,
        ALERT_ENDPOINTS["bus"],
        starttime=datetime.utcnow(),
        endtime=timedelta(seconds=10),
        realtime=True,
    )
