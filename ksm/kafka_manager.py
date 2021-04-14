import logging
import time
import socket

from kafka import KafkaConsumer
from ksm.defaults import Defaults

logger = logging.getLogger(__name__)


class kafkaSubs(object):
    def __init__(self, client_id, **kwargs):
        self.client_id = client_id
        self.host = kwargs.get("host", Defaults.KAFKA_HOST)
        self.port = kwargs.get("port", Defaults.KAFKA_PORT)
        self.loop_delay = int(kwargs.get("loop_delay", Defaults.KAFKA_LOOP_DELAY))
        self.auto_offset_reset = kwargs.get(
            "auto_offset_reset", Defaults.KAFKA_AUTO_OFFSET_RESET
        )
        self.stop_requested = False

    def get_topics(self):
        """
        Derived class must define the list of topics that will to subscribe.
        For example: ['topic1', 'topic2']
        """
        return []

    def disconnect(self):
        if self.conn:
            logger.info(f"disconnect from {self.host}:{self.port}")
            self.conn.close()
            self.conn = None

    def handle_messages(self):
        uri = f"{self.host}:{self.port}"
        topic = self.get_topics()
        if not len(topic):
            logger.info(f"There is no topic. There must be at least one topic.")
            return

        logger.info(f"connect to kafka server {uri}, topic: {topic}")
        try:
            self.conn = KafkaConsumer(
                *topic,
                client_id=self.client_id,
                bootstrap_servers=[uri],
                auto_offset_reset="latest",
            )

            while not self.stop_requested:
                # Response format is {TopicPartiton('topic1', 1): [msg1, msg2]}
                msg_pack = self.conn.poll(timeout_ms=self.loop_delay)
                for _, messages in msg_pack.items():
                    # message value and key are raw bytes -- decode if necessary!
                    # e.g., for unicode: `message.value.decode('utf-8')`
                    for m in messages:
                        logger.info(
                            f"[kafka message] topic: {m.topic}, parttition: {m.partition}, "
                            f"offset: {m.offset}, key: {m.key}, value: {m.value}"
                        )
                        self.handle_message(m)

        except (socket.error, socket.timeout, socket.herror) as e:
            logger.error(f"socket failure: {e}")
        except KeyboardInterrupt:
            pass
        finally:
            logger.info(f"shutdown kafka {self.client_id}")
            self.disconnect()

    def handle_message(self, m):
        # process a single message - derived classes should override this
        pass
