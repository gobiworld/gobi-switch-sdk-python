from core.datatypes import GobiKafkaEvent
from core.consumer import KafkaConsumerGobiSDK
from core.config import KafkaConsumerConfig
from core.base_handlers import BaseMessageHandler


class PrintHandler(BaseMessageHandler):
    """
    Example Kafka message handler.

    Users are expected to modify or replace this handler
    with their own business logic.
    """

    def handle(self, message: GobiKafkaEvent) -> None:
        print(message)


def main() -> None:
    """
    Application entrypoint.

    Initializes Kafka configuration and starts the consumer.
    """
    config = KafkaConsumerConfig()
    consumer = KafkaConsumerGobiSDK(config, handler=PrintHandler())
    consumer.start()


if __name__ == "__main__":
    main()
