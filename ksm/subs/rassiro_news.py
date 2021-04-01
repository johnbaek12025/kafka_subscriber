import json

from ksm.kafka_manager import kafkaSubs
from ksm.subscriber_manager import SubscriberManager


class Subscriber(kafkaSubs, SubscriberManager):
    def __init__(self, **kafka_opts):
        kafkaSubs.__init__(self, "rassiro_news", **kafka_opts)
        SubscriberManager.__init__(self)

        self.use_db = True
        self.db_mgrs = {"news_user_db": None}

        self.news_user_db = None

    def get_topics(self):
        return ["rassiro_news"]

    def initialize(self):
        self.news_user_db = self.db_mgrs["news_user_db"]

    def run(self):
        self.initialize()
        self.handle_messages()

    @staticmethod
    def get_message_format():
        return {
            "input": None,
            "news_sn": None,
            "ori_sn": None,
            "codes": None,
            "title": None,
            "img_url": None,
            "img_flag": None,
            "news_code": None,
            "ori_link": None,
            "source": None,
        }

    def handle_message(self, m):
        message = json.loads(m.value)       
        mf = Subscriber.get_message_format()
        proc_para = []
        for k in mf.keys():
            proc_para.append(message.get(k))

        cur = self.news_user_db.conn.cursor()

        out_result_cd = cur.var(int)
        proc_para.append(out_result_cd)
        out_result_msg = cur.var(str)
        proc_para.append(out_result_msg)

        self.news_user_db.callproc("PROC_RASSIRO_NEWS_INSERT", proc_para)
        logger.info(f"procedure return value {out_result_cd.getvalue()}, {out_result_msg.getvalue()})
