import json
import logging

from ksm.kafka_manager import kafkaSubs
from ksm.subscriber_manager import SubscriberManager


logger = logging.getLogger(__name__)


class Subscriber(kafkaSubs, SubscriberManager):
    def __init__(self, **kafka_opts):
        kafkaSubs.__init__(self, "rassiro_news", **kafka_opts)
        SubscriberManager.__init__(self)

        self.use_db = True
        self.db_mgrs = {"nu_db": None}

        self.nu_db = None

    def get_topics(self):
        return ["rassiro_news"]

    def initialize(self):
        self.nu_db = self.db_mgrs["nu_db"]

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

        cur = self.nu_db.conn.cursor()

        out_result_cd = cur.var(int)
        proc_para.append(out_result_cd)
        out_result_msg = cur.var(str)
        proc_para.append(out_result_msg)
        """
        입력예제 :
            PROC_RASSIRO_NEWS_INSERT(
                'I',                     -- P_INPUT   | 입력종류                  | I:입력, U:수정, D:삭제   
                '100000020210312151515', -- P_NEWS_SN | SN + 생성일 + 생성시       | SN NEWS_SN + D_NEWS_CRT + T_NEWS_CRT of RTBL_NEWS_INFO
                '100000020210312',       -- P_ORI_SN  | 원본뉴스SN + 생성일자      | ORI_NEWS_SN + D_ORI_NEWS_CRT of RTBL_NEWS_INFO
                '005930068270000660',    -- P_CODES   | 단축종목코드 + 관련종목코드 | RTBL_NEWS_INFO.STK_CODE  +RTBL_COM_RSC.RSC_CODE
                'TEST_TITLE',            -- 뉴스제목
                'test.com/test.jpg',     -- 대표이미지URL
                'N',                     -- 대표이미지가 있으면 Y 없으면 N
                'NS_BYU08',              -- 뉴스 코드 | EX: NS_BYU08
                '1111',                  -- 원본 데이터 식별값 (공시 RCPno 또는 리포트 SN 등)  
                19,                      -- 뉴스 source | 고정값 19
                P_RESULT_CD,             -- OUTPUT NUMBER
                P_RESULT_MSG             -- OUTPUT VARCHAR2(200)
            )
        """

        self.nu_db.callproc("PROC_RASSIRO_NEWS_INSERT", proc_para)
        logger.info(
            f"procedure return value {out_result_cd.getvalue()}, {out_result_msg.getvalue()}"
        )
