import json

from ksm.kafka_manager import kafkaSubs
from ksm.subscriber_manager import SubscriberManager


class Subscriber(kafkaSubs, SubscriberManager):
    def __init__(self, **kafka_opts):
        kafkaSubs.__init__(self, "test1", **kafka_opts)
        SubscriberManager.__init__(self)

        self.use_db = True
        self.db_mgrs = {"rc_db": None}

    def get_topics(self):
        return ["test1"]

    def run(self):
        self.handle_messages()

    def handle_message(self, m):
        m_dict = json.loads(m.value)

        result = self.db_mgrs["rc_db"].get_news_seq()
        print(result)