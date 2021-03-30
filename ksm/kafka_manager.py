import json
import logging
import time
import socket

from kafka import KafkaConsumer
from ksm.defaults import Defaults

logger = logging.getLogger(__name__)


class KafkaManager(object):
    def __init__(self, **kwargs):
        self.host = kwargs.get("host", Defaults.KAFKA_HOST)
        self.port = kwargs.get("port", Defaults.KAFKA_PORT)
        self.topic = kwargs.get("topic", Defaults.KAFKA_TOPIC)
        self.loop_delay = kwargs.get("port", Defaults.KAFKA_LOOP_DELAY)
        self.auto_offset_reset = kwargs.get("port", Defaults.KAFKA_LOOP_DELAY)

    def disconnect(self):
        if self.conn:
            logger.info(f"disconnect from {self.host}:{self.port}")
            self.conn.close()
            self.conn = None

    def handle_messages(self):
        uri = f"{self.host}:{self.port}"
        logger.info(f"connect to kafka server {uri}, topic: {self.topic}")
        try:
            self.conn = KafkaConsumer(
                *self.topic,
                bootstrap_servers=[uri],
                auto_offset_reset="latest",
            )

            for m in self.conn:
                logger.info(
                    f"[kafka message] topic: {m.topic}, parttition: {m.partition},  offset: {m.offset}, key: {m.key}, value: {m.value}"
                )

                m_dict = json.loads(m.value)
                print(m_dict)
        except (socket.error, socket.timeout, socket.herror) as e:
            logger.error(f"socket failure: {e}")
        except KeyboardInterrupt as e:
            logger.error(f"KeyboardInterrupt happened")
        except Exception as e:
            logger.error(f"kafka {uri} error: {e}")
