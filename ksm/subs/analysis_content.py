import json

from ksm.kafka_manager import kafkaSubs
from ksm.subscriber_manager import SubscriberManager


class Subscriber(kafkaSubs, SubscriberManager):
    def __init__(self, **kafka_opts):
        kafkaSubs.__init__(self, "analysis_content", **kafka_opts)
        SubscriberManager.__init__(self)

        self.use_db = True
        self.db_mgrs = {"news_user_db": None}

    def get_topics(self):
        return ["analysis_content"]

    def run(self):
        self.handle_messages()

    def handle_message(self, m):
        m_dict = json.loads(m.value)

        rc_db = self.db_mgrs["news_user_db"]

        cur = rc_db.conn.cursor()
        outVal = cur.var(int)
        self.db_mgrs["rc_db"].callproc("update_cyg", ["107590", outVal])
        print("1111", outVal.getvalue())
