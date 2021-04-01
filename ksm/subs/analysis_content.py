import json
import logging

from ksm.kafka_manager import kafkaSubs
from ksm.subscriber_manager import SubscriberManager


logger = logging.getLogger(__name__)


class Subscriber(kafkaSubs, SubscriberManager):
    def __init__(self, **kafka_opts):
        kafkaSubs.__init__(self, "analysis_content", **kafka_opts)
        SubscriberManager.__init__(self)

        self.use_db = True
        self.db_mgrs = {"tp_db": None}

        self.tp_db = None

    def get_topics(self):
        return ["analysis_content"]

    def initialize(self):
        self.tp_db = self.db_mgrs["tp_db"]

    def run(self):
        self.initialize()
        self.handle_messages()

    @staticmethod
    def get_message_format():
        return {
            "inp_kind": None,
            "sn": None,
            "ori_sn": None,
            "stkcode": None,
            "module_date": None,
            "module_time": None,
            "module_code": None,
            "module_name": None,
            "module_title": None,
            "module_summary": None,
            "module_cnts": None,
            "module_url": None,
        }

    def handle_message(self, m):
        message = json.loads(m.value)
        mf = Subscriber.get_message_format()
        proc_para = []
        for k in mf.keys():
            proc_para.append(message.get(k))

        """
        입력예제 :
            PROC_NTP_MODULE_DATA(
                'I',                          -- P_INP_KIND       | I: 신규뉴스, U:수정뉴스, D:삭제뉴스 
                '1000000020201212',           -- P_SN             | NEWS_SN + D_NEWS_CRT   
                '1000000020201212',           -- P_ORI_SN         | (원본글)NEWS_SN + (원본글)D_NEWS_CRT
                '056360',                     -- P_STKCODE        | 종목코드
                '20201212',                   -- P_MODULE_DATE    | 데이터 기준일자
                '121212',                     -- P_MODULE_TIME    | 데이터 기준시간 
                'ALS_BUY01_01',               -- P_MODULE_CODE    | 뉴스코드(=분석모듈 코드)
                '수급분석',                    -- P_MODULE_NAME    | 분석모듈 타입
                '<strong>삼성전자,</strong>',  -- P_MODULE_TITLE   | 뉴스제목(=분석모듈 좌측 상단 큰타이틀)
                '외국인 보유 비중이 확대되',    -- P_MODULE_SUMMARY | 분석모듈설명(=분석모듈 좌측 하단 설명글)
                '뉴스 본문 내용',              -- P_MODULE_CNTS    | 뉴스본문(=분석모듈 우측 내용)
                'URL'                         -- P_MODULE_URL     | 더보기 URL
            );
        """
        self.tp_db.callproc("PROC_NTP_MODULE_DATA", proc_para)
