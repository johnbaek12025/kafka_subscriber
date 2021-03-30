import logging
import time

from ksm.kafka_manager import KafkaManager

logger = logging.getLogger(__name__)


class KSException(Exception):
    def __init__(self, error_msg):
        super().__init__(error_msg)


# fixme: 시간날때, multiprocessing 처리
class KSManager(object):
    def __init__(self):
        self.kafka_manager = None

    def initialize(self, config_dict):
        kafka_info_dict = config_dict.get("kafka", {})
        self.kafka_manager = KafkaManager(**kafka_info_dict)

    def run(self):
        self.kafka_manager.handle_messages()
