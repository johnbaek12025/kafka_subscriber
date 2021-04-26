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
        self.db_mgrs = {"tp_db": None, "nu_db": None}

        self.tp_db = None
        self.nu_db = None

    def get_topics(self):
        return ["TpTpM1"]

    def initialize(self):
        self.tp_db = self.db_mgrs["tp_db"]
        self.nu_db = self.db_mgrs["nu_db"]

    def run(self):
        self.initialize()
        try:
            self.handle_messages()
        except Exception as err:
            logger.info(err)

    def handle_message(self, m):
        message = json.loads(m.value)
        logger.info(f"subcribed message: {message}")

        news_code = message.get("news_code")
        news_sn = message.get("news_sn")
        d_news_crt = message.get("d_news_crt")

        # 메세지 처리 여부 상관없이 무조건 완료 처리
        self.change_requests_status(news_sn, d_news_crt)

        if None in [news_code, news_sn, d_news_crt]:
            logger.info(
                f"Please check message: news_code: {news_code}, news_sn: {news_sn}, d_news_crt: {d_news_crt}"
            )
            return

        row = self.get_procedure_data(
            d_news_crt=d_news_crt, sn=news_sn, news_code=news_code
        )
        if row is None:
            return

        proc_para = list(row.values())
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

    def get_procedure_data(self, d_news_crt, sn, news_code):
        sql = f"""
            SELECT  A.NEWS_INP_KIND                     AS P_INP_KIND       
                    , A.NEWS_SN || A.D_NEWS_CRT         AS P_SN           
                    , A.ORI_NEWS_SN || A.D_ORI_NEWS_CRT AS P_ORI_SN
                    , A.STK_CODE                        AS P_STKCODE   
                    -- 인라인으로 하기 싫었지만, 상용과 개발을 혼합해서 사용해야 하는 상황이라...
                    , CASE WHEN A.IS_MANUAL = 0 THEN A.D_NEWS_CRT 
                      ELSE  (
                        SELECT  A.DATEDEAL
                        FROM    DATA_ENG.ALS_MAIN A
                                , (
                                    SELECT  INFO_CODE, INFO_SN
                                    FROM    RTBL_COM_ALS_INFOSN 
                                    WHERE   SN    = '{d_news_crt}'
                                    AND     D_CRT = {sn}
                                ) B
                        WHERE   A.SN        = B.INFO_SN
                        AND     A.INFO_CODE = B.INFO_CODE   
                    ) END AS P_MODULE_DATE 
                    , A.T_NEWS_CRT      AS P_MODULE_TIME
                    , A.NEWS_CODE       AS P_MODULE_CODE   
                    , B.ALS_TYPE        AS P_MODULE_NAME   
                    , A.NEWS_TITLE      AS P_MODULE_TITLE  
                    , B.ALS_DESC        AS P_MODULE_SUMMARY
                    , C.NEWS_CNTS       AS P_MODULE_CNTS
                    , C.RPST_IMG_URL    AS P_MODULE_URL   
            FROM    RTBL_NEWS_INFO A 
                    , RTBL_LUP_ALS_DESC B
                    , RTBL_NEWS_CNTS_ATYPE C                
            WHERE   1 = 1
            AND     A.D_NEWS_CRT    = '{d_news_crt}'
            AND     A.NEWS_SN       = {sn}  
            AND     B.ALS_NEWS_CODE = '{news_code.upper()}'
            AND     C.D_NEWS_CRT    = '{d_news_crt}'
            AND     C.NEWS_SN       = {sn}  
        """
        rows = self.nu_db.get_all_rows(sql)
        if not rows:
            return None
        r = rows[0]
        return {
            "inp_kind": r[0],
            "sn": r[1],
            "ori_sn": r[2],
            "stkcode": r[3],
            "module_date": r[4],
            "module_time": r[5],
            "module_code": r[6],
            "module_name": r[7],
            "module_title": r[8],
            "module_summary": r[9],
            "module_cnts": r[10].read(),
            "module_url": r[11],
        }

    def change_requests_status(self, news_sn, d_news_crt, status="S", commit=True):
        sql = f"""
            UPDATE RTBL_NEWS_INFO
            SET    ADM_SEND_STATUS = '{status}'
            WHERE  NEWS_SN = {news_sn}
            AND    D_NEWS_CRT = '{d_news_crt}'
        """
        self.nu_db.modify(sql, commit)