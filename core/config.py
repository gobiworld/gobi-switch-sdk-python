# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
import os
import socket


class KafkaConsumerConfig:
    """Configuration centralisée, majoritairement via variables d'environnement."""

    def __init__(self, **kwargs):
        self.topic = os.getenv("KAFKA_TOPIC")
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        self.group_id = os.getenv("KAFKA_GROUP_ID")
        self.client_id = os.getenv("KAFKA_CLIENT_ID", socket.gethostname())

        self.auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        self.api_version = tuple(
            int(v) for v in os.getenv("KAFKA_API_VERSION", "3.8").split(".")
        )

        self.security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        self.sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")

        # SCRAM
        self.sasl_username = os.getenv("KAFKA_SASL_USERNAME")
        self.sasl_password = os.getenv("KAFKA_SASL_PASSWORD")

        # IAM / OAUTH
        self.aws_region = os.getenv("AWS_REGION", "eu-west-3")

        # Batch
        self.batch_size = int(os.getenv("KAFKA_BATCH_SIZE", "1"))
        self.batch_timeout = float(os.getenv("KAFKA_BATCH_TIMEOUT", "1.0"))

        # API
        self.switch_api_host = os.getenv("SWITCH_API_HOST", "http://127.0.0.1:8000")
        self.switch_api_public_key = os.getenv("SWITCH_API_PUBLIC_KEY")
        self.switch_api_private_key = os.getenv("SWITCH_API_PRIVATE_KEY")
        self.switch_api_mode = os.getenv("SWITCH_API_MODE", "live")

        if not self.switch_api_public_key or not self.switch_api_private_key:
            raise ValueError("Both SWITCH_API_PUBLIC_KEY and SWITCH_API_PRIVATE_KEY must be set")

        if not self.topic or not self.bootstrap_servers:
            raise ValueError("KAFKA_TOPIC et KAFKA_BOOTSTRAP_SERVERS sont obligatoires")