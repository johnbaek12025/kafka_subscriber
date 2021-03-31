import ksm.db_manager


class DBManager(ksm.db_manager.DBManager):
    def __init__(self):
        pass

    def get_news_seq(self):
        sql = """
            -- SELECT  NEWS_USER.RTBL_NEWS_SEQUENCE.NEXTVAL
            SELECT  CYG_RTBL_NEWS_SEQUENCE.NEXTVAL
            FROM    DUAL
        """
        rows = self.get_all_rows(sql)
        if not rows:
            return None
        return rows[0][0]