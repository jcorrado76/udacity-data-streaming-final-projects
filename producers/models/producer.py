"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # these properties are specified in the documentation at:
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.avro.AvroProducer
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "schema.registry.url": SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # the constructor for AvroProducer is described at:
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.avro.AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """
        Creates the producer topic if it does not already exist
        """
        admin_client = AdminClient(
            {
                "bootstrap.servers": BROKER_URL
            }
        )

        exists = Producer.topic_exists(admin_client, self.topic_name)
        logging.info(f"Topic {self.topic_name} exists: {exists}")

        if not exists:
            self._create_topic(admin_client)

        logger.info("topic creation kafka integration incomplete - skipping")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.commit_transaction(timeout=5)

    def _create_topic(self, client: confluent_kafka.admin.AdminClient) -> None:
        """
        Creates the topic with the given topic name

        See the documentation on NewTopic here:
        https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic

        See the documentation on topic configuration here:
        https://docs.confluent.io/current/installation/configuration/topic-configs.html
        """
        futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logging.info(f"Topic: {self.topic_name} created")
            except Exception as e:
                logging.info(f"Failed to create topic: {self.topic_name}: {e}")
                raise

    @staticmethod
    def topic_exists(client: confluent_kafka.admin.AdminClient, topic_name: str) -> bool:
        """
        Checks if the given topic exists.

        Documentation on list_topics can be found here:
        https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.list_topics
        """
        topic_metadata = client.list_topics(timeout=5)
        return topic_metadata.topics.get(topic_name) is not None


def time_now_in_millis() -> int:
    """
    Use this function to get the key for Kafka Events
    """
    return int(round(time.time() * 1000))
