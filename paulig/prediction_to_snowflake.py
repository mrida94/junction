import pandas as pd
import json
from azure.eventhub import EventHubClient
from azure.eventhub import EventData
from datetime import datetime

client = EventHubClient.from_connection_string(connection_str)
sender = client.add_sender(partition="0")

df_paulig = pd.read_csv("paulig_model_results.csv")
id = 0

client.run()

for row_dict in df_paulig.to_dict(orient="records"):
    id = id+1

    event = {
          "event_data": row_dict,
          "event_domain": "TEAM_04",
          "event_id": id,
          "event_source": "junction_app_test",
          "event_ts": datetime.now().isoformat(),
          "event_type": "forecast_result"
        }

    event = json.dumps(event).encode('utf-8')

    event_data = EventData(event)
    sender.send(event_data)

client.stop()