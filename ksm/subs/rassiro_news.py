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
        return ["TpTpW1"]

    def initialize(self):
        self.nu_db = self.db_mgrs["nu_db"]

    def run(self):
        self.initialize()
        self.handle_messages()

    def handle_message(self, m):
        message = json.loads(m.value)

        news_code = message.get("news_code")
        news_sn = message.get("news_sn")
        d_news_crt = message.get("d_news_crt")
        if None in [news_code, news_sn, d_news_crt]:
            logger.info(
                f"Please check message: news_code: {news_code}, news_sn: {news_sn}, d_news_crt: {d_news_crt}"
            )
            return

        logger.info(f"subcribed message: {message}")
        row = self.get_procedure_data(
            d_news_crt=d_news_crt, sn=news_sn, news_code=news_code
        )
        if row is None:
            return

        proc_para = list(row.values())

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

    def get_procedure_data(self, d_news_crt, sn, news_code):
        sql = f"""
            SELECT  'I'                                               AS P_INPUT -- I:입력, U:수정, D:삭제 
                    , B.NEWS_SN || B.D_NEWS_CRT || B.T_NEWS_CRT       AS P_NEWS_SN  
                    , B.ORI_NEWS_SN || B.D_ORI_NEWS_CRT               AS P_ORI_SN        
                    , B.STK_CODE || C.RSC_CODE                        AS P_CODES -- 종목코드
                    , B.NEWS_TITLE                                    AS P_TITLE  
                    , DECODE(A.RPST_IMG_URL, 'N', '', A.RPST_IMG_URL) AS P_IMG_URL 
                    , DECODE(A.RPST_IMG_URL, 'N', 'N', 'Y')           AS P_IMG_FLAG
                    , A.NEWS_CODE                                     AS P_NEWS_CODE
                    , ''                                              AS P_ORI_LINK_SN -- 원본 데이터 식별값 (공시 RCPno 또는 리포트 SN 등)  
                    , 19                                              AS P_SOURCE --  뉴스 source | 고정값 19   
            FROM    RTBL_NEWS_CNTS_ATYPE A,
                    RTBL_NEWS_INFO B,
                    RTBL_COM_RSC C
            WHERE   A.D_NEWS_CRT = '{d_news_crt}'
            AND     A.NEWS_SN    = {sn}
            AND     B.D_NEWS_CRT = '{d_news_crt}'
            AND     B.NEWS_SN    = {sn}
            AND     C.D_CRT      = '{d_news_crt}'
            AND     C.SN         = {sn}      
        """
        rows = self.nu_db.get_all_rows(sql)
        if not rows:
            return None
        r = rows[0]
        return {
            "input": r[0],
            "news_sn": r[1],
            "ori_sn": r[2],
            "codes": r[3],
            "title": r[4],
            "img_url": r[5],
            "img_flag": r[6],
            "news_code": r[7],
            "ori_link_sn": r[8],
            "source": r[9],
        }