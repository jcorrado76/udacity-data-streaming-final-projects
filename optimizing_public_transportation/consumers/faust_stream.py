"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
input_topic = app.topic("org.chicago.cta.stations", value_type=Station)
output_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1)

table = app.Table(
   name="transformed_stations",
   default=TransformedStation,
   partitions=1,
   changelog_topic=output_topic,
)


@app.agent(input_topic)
async def transform_station_events(station_events):
    async for station_event in station_events:
        if station_event.red:
            line = 'red'
        elif station_event.blue:
            line = 'blue'
        else:
            line = 'green'

        transformed_station_event = TransformedStation(
            station_event.station_id,
            station_event.station_name,
            station_event.order,
            line
        )

        table[transformed_station_event.station_id] = transformed_station_event


if __name__ == "__main__":
    app.main()
