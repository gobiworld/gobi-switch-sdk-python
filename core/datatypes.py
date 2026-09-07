from dataclasses import dataclass, field
from typing import List


from dataclasses import dataclass
from typing import Any, Optional

from pydantic.v1 import UUID4

from openapi_client import Transaction


@dataclass(frozen=True)
class GobiKafkaValue:
    """Class representing GobiTransferEvent"""
    transactionUuid: UUID4
    timestamp: int
    amount: bytes
    amountUsd: List[str]
    earnings: List[bytes]
    earningsUsd: str

    @staticmethod
    def parse(attribute_value_dict):
        return GobiKafkaValue(**attribute_value_dict)


@dataclass(frozen=True)
class GobiKafkaEvent:
    """Normalized Kafka event passed to handlers."""
    key: Optional[Any]
    value: GobiKafkaValue
    topic: str
    partition: int
    offset: int
    timestamp: Optional[int]
    headers: list
    transaction: Optional[Transaction] = None