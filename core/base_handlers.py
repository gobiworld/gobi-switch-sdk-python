# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------
from abc import ABC, abstractmethod
from typing import List
from .datatypes import GobiKafkaEvent


class BaseMessageHandler(ABC):
    """Handler unitaire."""

    @abstractmethod
    def handle(self, message: GobiKafkaEvent) -> None:
        pass


class BaseBatchHandler(ABC):
    """Handler batch."""

    @abstractmethod
    def handle_batch(self, messages: List[GobiKafkaEvent]) -> None:
        pass

