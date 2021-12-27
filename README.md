# Optimizing Public Transportation Project
***
This is the codebase for my implementation of the Udacity Optimizing Public Transportation Project from the Data Streaming Nanodegree program.

The high level architecture for this application looks like this:
![high level architecture](images/Optimizing%20Public%20Transportation%20Project.png)

The objective for this project was to produce a dashboard displaying weather conditions,
train arrival times between stations, as well as counts of turnstile turns at each station.

For the frontend, we leveraged the Tornado server platform and Kafka consumers to ingest the data from
our Kafka cluster.

In terms of data processing, we utilized a Kafka cluster as our message broker, as well as Faust and KSQL to process the data.

In this project, we had three main data sources:
* a Weather HTTP source
* a PostgreSQL database containing static information about the stations
* Train Arrival and Turnstile turn information

The weather, train arrival, and turnstile turn data is simulated and posted to the Kafka cluster
using some Python code, and the static station information is created in a local PostgreSQL database deployment.

The final dashboard will look like this:
![Final User Interface](images/ui.png) 
# Contents
***
## Producers
### Producer Base Class
For producers, the first thing we needed to implement was the `Producer` base class, found in `producers/models/producer.py`

The `producer` base class handles creating the topic for this producer, as well as instantiating the underlying `Producer` class
from the python Confluent library.
### Train Arrivals and Turnstile Events
Next, we needed to define the Avro schema for train arrival events in `/producers/models/schemas/arrival_value.json`.

1. Define a `value` schema for the arrival event in `producers/models/schemas/arrival_value.json` with the following attributes
	* `station_id`
	* `train_id`
	* `direction`
	* `line`
	* `train_status`
	* `prev_station_id`
	* `prev_direction`
1. Complete the code in `producers/models/station.py` so that:
	* A topic is created for each station in Kafka to track the arrival events
	* The station emits an `arrival` event to Kafka whenever the `Station.run()` function is called.
	* Ensure that events emitted to kafka are paired with the Avro `key` and `value` schemas
1. Define a `value` schema for the turnstile event in `producers/models/schemas/turnstile_value.json` with the following attributes
	* `station_id`
	* `station_name`
	* `line`
1. Complete the code in `producers/models/turnstile.py` so that:
	* A topic is created for each turnstile for each station in Kafka to track the turnstile events
	* The station emits a `turnstile` event to Kafka whenever the `Turnstile.run()` function is called.
	* Ensure that events emitted to kafka are paired with the Avro `key` and `value` schemas
### Configure REST Proxy for Weather Events
In addition, we needed to complete the code inside `producers/models/weather.py`, as well as the event schema for weather events
inside `producers/models/schemas/weather_value.json` to define the weather events, and then configure
Python with the `requests` library to make the POST requests to create data.

1. Define a `value` schema for the weather event in `producers/models/schemas/weather_value.json` with the following attributes
	* `temperature`
	* `status`
1. Complete the code in `producers/models/weather.py` so that:
	* A topic is created for weather events
	* The weather model emits `weather` event to Kafka REST Proxy whenever the `Weather.run()` function is called.
		* **NOTE**: When sending HTTP requests to Kafka REST Proxy, be careful to include the correct `Content-Type`. Pay close attention to the [examples in the documentation](https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)) for more information.
	* Ensure that events emitted to REST Proxy are paired with the Avro `key` and `value` schemas
### Postgres Kafka Connector
The first consumer we'll work on is the Kafka Connect JDBC Connector to read the static station data from the PostgreSQL 
database we have. 

1. Complete the code and configuration in `producers/connectors.py`
    * Please refer to the [Kafka Connect JDBC Source Connector Configuration Options](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html) for documentation on the options you must complete.
    * You can run this file directly to test your connector, rather than running the entire simulation.
    * Make sure to use the [Landoop Kafka Connect UI](http://localhost:8084) and [Landoop Kafka Topics UI](http://localhost:8085) to check the status and output of the Connector.
    * To delete a misconfigured connector: `CURL -X DELETE localhost:8083/connectors/stations`

When creating the JDBC Kafka connector, there are several properties inside the `configuration` field of the POST request we make
to create the connector for our database.
In particular, the topic name that the output data is sent to is generated as `f"{topic.prefix}[table-name]"`.
In our case, since we were parsing a table called `stations`, and we specified a `topic.prefix` of `org.chicago.cta.`, the data
coming from the `stations` table went to a topic called `org.chicago.cta.stations`.
## Transformations
These two sections are about how we used the processing frameworks of Faust and KSQL to transform the events that were generated
into the topics created from our three data sources. Technically, these processing frameworks are also consumers of those topics,
but they are more like intermediate consumers, and output to new streams. The consumers section will detail the actual "consumers"
that can be found in the final application. 
### Faust Stream Processor
We used the Faust framework to transform the raw, static station data from our Postgres database, which we ingested using Kafka
Connect.

In this case, we reduced the amount of information in those events to only the data we need, and we changed some of the 
color formatting in those events.

This is configured inside `consumers/faust_stream.py`.

You run the Faust application with:
```bash
faust -A faust_stream worker -l info 
```

For this consumer, we outputted the results of our transformation and filtering to the `org.chicago.cta.stations.table.v1` topic.
### KSQL Table
All we needed to do here was complete the SQL queries in `consumers/ksql.py` to read in data from an existing Kafka Stream, and then create
a new output stream of that data. 

In this case, we were utilizing the turnstile data, and aggregating it together, before publishing the results to another output stream.

The output topic name is just the name of KSQL table we created to do the aggregation.
In this case, the table name was `TURNSTILE_SUMMARY`. So, when we consume the output of this aggregation, that's the topic
name we'll be looking for.
## Consumers
Now that we've created all the data we'll need inside various topics on our Kafka cluster, we need to create the Python consumer
code to actually consume data from the cluster into the application.

The first module we'll complete is the `consumers/consumer.py` file. This file defines the consumer base class (`KafkaConsumer`) for the consumers
of the various topics. Note that although we had very different data sources, and producers to create that data, all of the final
data is just events in kafka topics. So we only need to implement logic as to how to parse those output events, and deserialize
the information according to the schemas passed and the encoding used. 

The base `KafkaConsumer` takes care of initializing a connection to the Kafka cluster, and instantiating an underlying Python Confluent
`Consumer` class. 
Something to be noted here is that the constructor of the `KafkaConsumer` base class takes in a `topic_name_pattern` to define which
streams to pull data from. In fact, this pattern can use regular expressions.

For example, to pull data from any stream (which we implemented to have separate train arrival streams for each train station), you could
have a topic pattern like this: `"^org.chicago.cta.station.arrivals.*"`, where the preceding `^` indicates that we do not want an 
exact string match on our pattern, but we want to treat it as a regex.

The regular expression patterns supported here are documented at https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/RegExp.

Finally, we needed to edit the files:
* `consumers/models/line.py`
* `consumers/models/weather.py`
* `consumers/models/station.py`

To implement a `process_message` function, which identifies the logic as to how to serialize the data that is being consumed by the
various consumers. This happens in the `process_message` method of the consumer classes.

For example, our `Line` class and `Station` class know how to `_handle_arrival`, `_handle_station`, and handle incoming turnstile counts if the incoming event's topic is 
`org.chicago.cta.station.arrivals.v1`, `org.chicago.cta.stations`, or `TURNSTILE_SUMMARY`, respectively.

And our `Weather` class in `consumers/models/weather.py` knows how to deserialize the fields of the weather events that we need
for our frontend. 
### Step 6: Create Kafka Consumers
With all of the data in Kafka, our final task is to consume the data in the web server that is going to serve the transit status pages to our commuters.

To accomplish this, you must complete the following tasks:

1. Complete the code in `consumers/consumer.py`
2. Complete the code in `consumers/models/line.py`
3. Complete the code in `consumers/models/weather.py`
4. Complete the code in `consumers/models/station.py`

## Additional Resources
Here are some additional links with information about using Kafka.

* [Confluent Python Client Documentation](https://docs.confluent.io/current/clients/confluent-kafka-python/#)
* [Confluent Python Client Usage and Examples](https://github.com/confluentinc/confluent-kafka-python#usage)
* [REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name))
* [Kafka Connect JDBC Source Connector Configuration Options](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html)

## Directory Layout
The project consists of two main directories, `producers` and `consumers`.

The following directory layout indicates the files that the student is responsible for modifying by adding a `*` indicator. Instructions for what is required are present as comments in each file.

```
* - Indicates that the student must complete the code in this file

├── consumers
│   ├── consumer.py *
│   ├── faust_stream.py *
│   ├── ksql.py *
│   ├── models
│   │   ├── lines.py
│   │   ├── line.py *
│   │   ├── station.py *
│   │   └── weather.py *
│   ├── requirements.txt
│   ├── server.py
│   ├── topic_check.py
│   └── templates
│       └── status.html
└── producers
    ├── connector.py *
    ├── models
    │   ├── line.py
    │   ├── producer.py *
    │   ├── schemas
    │   │   ├── arrival_key.json
    │   │   ├── arrival_value.json *
    │   │   ├── turnstile_key.json
    │   │   ├── turnstile_value.json *
    │   │   ├── weather_key.json
    │   │   └── weather_value.json *
    │   ├── station.py *
    │   ├── train.py
    │   ├── turnstile.py *
    │   ├── turnstile_hardware.py
    │   └── weather.py *
    ├── requirements.txt
    └── simulation.py
```

## Running and Testing

To run the simulation locally, you must first start up the Kafka ecosystem on your local machine with Docker Compose:
```bash
docker-compose up
```

This leverages the local `docker-compose.yaml` file to create all the resources you need to run this simulation locally. 



Docker compose will take a 3-5 minutes to start, depending on your hardware. Please be patient and wait for the docker-compose logs to slow down or stop before beginning the simulation.

Once docker-compose is ready, the following services will be available:

| Service | Host URL | Docker URL | Username | Password |
| --- | --- | --- | --- | --- |
| Public Transit Status | [http://localhost:8888](http://localhost:8888) | n/a | ||
| Landoop Kafka Connect UI | [http://localhost:8084](http://localhost:8084) | http://connect-ui:8084 |
| Landoop Kafka Topics UI | [http://localhost:8085](http://localhost:8085) | http://topics-ui:8085 |
| Landoop Schema Registry UI | [http://localhost:8086](http://localhost:8086) | http://schema-registry-ui:8086 |
| Kafka | PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094 | PLAINTEXT://kafka0:9092,PLAINTEXT://kafka1:9093,PLAINTEXT://kafka2:9094 |
| REST Proxy | [http://localhost:8082](http://localhost:8082/) | http://rest-proxy:8082/ |
| Schema Registry | [http://localhost:8081](http://localhost:8081/ ) | http://schema-registry:8081/ |
| Kafka Connect | [http://localhost:8083](http://localhost:8083) | http://kafka-connect:8083 |
| KSQL | [http://localhost:8088](http://localhost:8088) | http://ksql:8088 |
| PostgreSQL | `jdbc:postgresql://localhost:5432/cta` | `jdbc:postgresql://postgres:5432/cta` | `cta_admin` | `chicago` |

Note that to access these services from your own machine, you will always use the `Host URL` column.

When configuring services that run within Docker Compose, like **Kafka Connect you must use the Docker URL**. When you configure the JDBC Source Kafka Connector, for example, you will want to use the value from the `Docker URL` column.

### Running the Simulation
In general, you need to start up the components making up the three sections:
* producers - `simulation.py`
* transformers:
  * `faust_stream.py`
  * `ksql.py`
* consumers - `server.py`

Your producers, and your transformation code needs to already be running when you run `server.py`, or it won't work.

#### To run the `producer`:

1. `cd producers`
2. `virtualenv venv`
3. `. venv/bin/activate`
4. `pip install -r requirements.txt`
5. `python simulation.py`

Once the simulation is running, you may hit `Ctrl+C` at any time to exit.

#### To run the Faust Stream Processing Application:
1. `cd consumers`
2. `virtualenv venv`
3. `. venv/bin/activate`
4. `pip install -r requirements.txt`
5. `faust -A faust_stream worker -l info`


#### To run the KSQL Creation Script:
1. `cd consumers`
2. `virtualenv venv`
3. `. venv/bin/activate`
4. `pip install -r requirements.txt`
5. `python ksql.py`

#### To run the `consumer`:
1. `cd consumers`
2. `virtualenv venv`
3. `. venv/bin/activate`
4. `pip install -r requirements.txt`
5. `python server.py`

Once the server is running, you may hit `Ctrl+C` at any time to exit.
# Closing Thoughts
This was an interesting project that leveraged several input sources, performed some trivial data transformations, and then consumed that
data in a frontend.

It was essential to make sure that I kept an eye on the high level design of the architecture to remind myself of which components
plug into which other components. This is partially why I created the high level architecture diagram.

One of the pain points that caused some trouble for me was making sure that the topics that were being created lined up with the topics that
were being consumed by the transformation frameworks, and the consumers inside our application. 

Refactoring and pre-creating these topics might have helped to alleviate the concerns in terms of lining up the topic names, but creating
the diagram was indispensable and immediately helped to remedy some maligned topic names.

