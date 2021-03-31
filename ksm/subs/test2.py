import json

from ksm.kafka_manager import kafkaSubs


class Subscriber(kafkaSubs):
    def __init__(self, **kafka_opts):
        super(Subscriber, self).__init__("test2", **kafka_opts)
        self.use_db = True
        self.db_mgr = None

    def get_topics(self):
        return ["test2"]

    def run(self):
        self.handle_messages()

    def handle_message(self, m):
        m_dict = json.loads(m.value)
        print(m_dict)