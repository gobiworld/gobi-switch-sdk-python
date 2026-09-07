"""
Kafka Consumer SDK
==================

Objectif:
- Simplifier l'intégration de consumers Kafka
- Permettre une forte customisation des handlers (unitaire et batch)
- Supporter plusieurs modes d'authentification (SASL SCRAM, IAM / OAUTHBEARER)
- Centraliser la configuration via des variables d'environnement

Inspiré du pattern du consumer SQS HexmosTech.
"""

import json
import logging
from typing import Any, Callable, List, Optional, Tuple

from kafka import KafkaConsumer
from pydantic.v1 import UUID4
from openapi_client import Configuration, ApiClient, TransactionApi
from openapi_client.models import Transaction, Status44dEnum, TransactionCallbackRequest

from .datatypes import GobiKafkaEvent, GobiKafkaValue
from .iam_auth_provider import IAMTokenProvider
from .base_handlers import BaseMessageHandler, BaseBatchHandler
from .config import KafkaConsumerConfig

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka-consumer-sdk")


# ---------------------------------------------------------------------------
# Consumer SDK
# ---------------------------------------------------------------------------
class KafkaConsumerGobiSDK:
    """SDK Kafka Consumer."""

    def __init__(
        self,
        config: KafkaConsumerConfig,
        handler: Optional[BaseMessageHandler] = None,
        batch_handler: Optional[BaseBatchHandler] = None,
        value_deserializer: Optional[Callable[[bytes], Any]] = None,
    ):
        if handler and batch_handler:
            raise ValueError("Choisir soit handler soit batch_handler, pas les deux")

        self.config = config
        self.handler = handler
        self.batch_handler = batch_handler
        self.value_deserializer = (
            value_deserializer
            or (lambda v: json.loads(v.decode("utf-8")))
        )

        if self.batch_handler:
            raise ValueError("'batch_handler' is not stable yet. Please use 'handler' instead")

        self.consumer = self._build_consumer()
        logger.debug(f"switch_api_host: {self.config.switch_api_host}")
        configuration = Configuration(
            host=self.config.switch_api_host,
            api_key={
                "PublicApiKeySchema": self.config.switch_api_public_key,
                "PrivateApiKeySchema": self.config.switch_api_private_key,
                "SandboxApiKeySchema": self.config.switch_api_mode,
            }
        )
        self.api_client = ApiClient(configuration)
        self.api = TransactionApi(self.api_client)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def _build_consumer(self) -> KafkaConsumer:
        kwargs = dict(
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.group_id,
            client_id=self.config.client_id,
            auto_offset_reset=self.config.auto_offset_reset,
            api_version=self.config.api_version,
            value_deserializer=self.value_deserializer,
            security_protocol=self.config.security_protocol,
        )

        # Auth SCRAM
        if self.config.sasl_mechanism and self.config.sasl_mechanism.startswith("SCRAM"):
            kwargs.update(
                sasl_mechanism=self.config.sasl_mechanism,
                sasl_plain_username=self.config.sasl_username,
                sasl_plain_password=self.config.sasl_password,
            )

        # Auth IAM / OAUTH
        elif self.config.sasl_mechanism == "OAUTHBEARER":
            kwargs.update({
                "sasl_mechanism": "OAUTHBEARER",
                "sasl_oauth_token_provider": IAMTokenProvider(
                    self.config.aws_region
                )
            })

        return KafkaConsumer(self.config.topic, **kwargs)

    # ------------------------------------------------------------------
    # Runtime
    # ------------------------------------------------------------------
    def start(self) -> None:
        logger.info("Kafka consumer started")

        if self.batch_handler:
            self._consume_batch()
        else:
            self._consume_single()

    def _build_event(self, msg, transaction: Transaction) -> GobiKafkaEvent:
        event = GobiKafkaEvent(
            key=msg.key,
            value=GobiKafkaValue(**msg.value),
            topic=msg.topic,
            partition=msg.partition,
            offset=msg.offset,
            timestamp=msg.timestamp,
            headers=msg.headers,
            transaction=transaction
        )
        return event

    def _consume_single(self) -> None:
        for msg in self.consumer:
            _msg_treated_successfully = False
            event: Optional[GobiKafkaEvent] = None
            print("msg_kafka: ", msg)
            try:
                transaction_uuid = bytes(msg.key).decode("utf-8").split("--")[0]
                print("transaction_uuid: ", transaction_uuid)
                _is_valid, _data = self.is_valid(UUID4(transaction_uuid))
                event: GobiKafkaEvent = self._build_event(msg, transaction=_data)

                if _is_valid:
                    self.handler.handle(event)
                    _msg_treated_successfully = True
                else:
                    logger.error(f"Event is invalid. Exception: {_data}")
            except Exception as e:
                logger.error(f"Erreur lors du traitement du message. Exception: {e}")
            if _msg_treated_successfully:
                self._payout_callback(event)

    def _consume_batch(self) -> None:
        raise NotImplementedError()
        # buffer: List[GobiKafkaEvent] = []
        #
        # for msg in self.consumer:
        #     event = self._build_event(msg)
        #     _is_valid, _exception = self.is_valid(event)
        #     if _is_valid:
        #         buffer.append(event)
        #     else:
        #         logger.info(f"Event is invalid. Exception: {_exception}")
        #
        #     if len(buffer) >= self.config.batch_size:
        #         self._flush_batch(buffer)
        #         buffer.clear()

    def _flush_batch(self, batch: List[GobiKafkaEvent]) -> None:
        try:
            self.batch_handler.handle_batch(batch)
        except Exception:
            logger.exception("Erreur lors du traitement du batch")

    # # Permettre à l'utilisateur de passer les valeur de config directement. Si None utiliser vérifier si c'est
    # # dans les variables d'environnement
    # # Permettre à l'utilisateur de spécifier d'autres variables de kafka qu'il souhaiterait passer
    # # (autre que ceux qui sont déjà demandé, pas besoin de check. On part du principe seul les pro peuvent manipullée ces nouveau params)

    def is_valid(self, transaction_uuid: UUID4) -> Tuple[bool, Optional[Any]]:
        transaction: Transaction = self.api.api_paiement_transactions_retrieve(transaction_uuid)
        logger.debug(f"transaction_detail: {transaction}")
        if transaction.status == Status44dEnum.PAYOUT_ORDERED:
            return True, transaction
        return False, f"Transaction({transaction_uuid}) | status {transaction.status}"


    def _payout_callback(self, event: GobiKafkaEvent) -> None:
        log_prefixe = f"Transaction({event.value.transactionUuid})"
        transaction_callback = self.api.api_paiement_transactions_payout_callback_create(
            TransactionCallbackRequest(transaction_uuid=event.value.transactionUuid))
        if transaction_callback.status == Status44dEnum.PAYOUT_DONE:
            logger.info(f"{log_prefixe} | Payout callback executed successfully")
        else:
            logger.error(f"{log_prefixe} | Payout callback failed, compensation not applied")

# ---------------------------------------------------------------------------
# Exemples d'utilisation
# ---------------------------------------------------------------------------
"""
# Handler simple
class PrintHandler(BaseMessageHandler):
    def handle(self, message):
        print(message)

config = KafkaConsumerConfig()
consumer = KafkaConsumerGobiSDK(config, handler=PrintHandler())
consumer.start()

# Handler batch
class BatchPrintHandler(BaseBatchHandler):
    def handle_batch(self, messages):
        print(f"Batch size: {len(messages)}")

consumer = KafkaConsumerGobiSDK(config, batch_handler=BatchPrintHandler())
consumer.start()
"""

