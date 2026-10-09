from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from django.conf import settings
from django.core.management.base import BaseCommand

from config import kafka_topics


class Command(BaseCommand):
    help = "Creates Kafka topics used by the order system (idempotent)"

    def handle(self, *args, **options):
        admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})

        new_topics = [
            NewTopic(
                name,
                num_partitions=kafka_topics.PARTITIONS,
                replication_factor=kafka_topics.REPLICATION_FACTOR,
            )
            for name in kafka_topics.ALL_TOPICS
        ]

        for name, future in admin.create_topics(new_topics).items():
            try:
                future.result()
                self.stdout.write(self.style.SUCCESS(f"Created topic {name}"))
            except KafkaException as exc:
                if exc.args[0].name() == "TOPIC_ALREADY_EXISTS":
                    self.stdout.write(f"Topic {name} already exists")
                else:
                    raise
