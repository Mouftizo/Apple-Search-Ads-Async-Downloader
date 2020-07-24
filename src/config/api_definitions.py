import json
import datetime


def modify_report(start_time: datetime.time, end_time: datetime.time) -> dict:
    report = {
        "startTime": start_time.strftime('%Y-%m-%d'),
        "endTime": end_time.strftime('%Y-%m-%d'),
        "selector": {
            "orderBy": [
                {
                    "field": "countryOrRegion",
                    "sortOrder": "ASCENDING"
                }
            ],
            "pagination": {
                "offset": 0,
                "limit": 1000
            }
        },
        "groupBy": [
            "countryOrRegion"
        ],
        "timeZone": "UTC",
        "returnRecordsWithNoMetrics": True,
        "returnRowTotals": True,
        "returnGrandTotals": True
    }

    serialize = json.dumps(report)
    deserialize = json.loads(serialize)
    return deserialize
